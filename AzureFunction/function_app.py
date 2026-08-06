import datetime
import hashlib
import json
import logging
import math
import os
import time
import uuid
from io import BytesIO

import azure.functions as func
import bcrypt
import jwt
import pandas as pd
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.storage.blob import BlobServiceClient


# Initialize the Azure Functions application.
app = func.FunctionApp()


# Blob Storage configuration.
CONTAINER_NAME = "diet-analysis"
RAW_BLOB_NAME = "All_Diets.csv"
CLEAN_BLOB_NAME = "Clean_Diets.csv"

# Cosmos DB configuration.
DATABASE_NAME = "DietAnalysisDB"
CACHE_CONTAINER_NAME = "Cache"
USERS_CONTAINER_NAME = "Users"

# JWT secret is loaded from local.settings.json or Azure App Settings.
JWT_SECRET = os.environ.get("JWT_SECRET", "")


def json_response(data, status_code=200):
    """
    Create a consistent JSON HTTP response.

    Args:
        data: The response body to serialize as JSON.
        status_code: HTTP status code.

    Returns:
        An Azure Functions HTTP response.
    """
    return func.HttpResponse(
        json.dumps(data),
        status_code=status_code,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin": "*",
        },
    )


def get_cosmos_container(container_name):
    """
    Connect to Cosmos DB and return the requested container.

    The database and container are automatically created if they
    do not already exist.

    Args:
        container_name: Name of the Cosmos DB container.

    Returns:
        A Cosmos DB ContainerProxy object.
    """
    connection_string = os.environ["COSMOS_CONNECTION_STRING"]

    client = CosmosClient.from_connection_string(
        connection_string
    )

    database = client.create_database_if_not_exists(
        id=DATABASE_NAME
    )

    return database.create_container_if_not_exists(
        id=container_name,
        partition_key=PartitionKey(path="/id"),
    )


def get_blob_service():
    """
    Create and return an Azure Blob Service client.
    """
    connection_string = os.environ[
        "AZURE_STORAGE_CONNECTION_STRING"
    ]

    return BlobServiceClient.from_connection_string(
        connection_string
    )


@app.blob_trigger(
    arg_name="myblob",
    path=f"{CONTAINER_NAME}/{RAW_BLOB_NAME}",
    connection="AzureWebJobsStorage",
)
def CleanAndCache(myblob: func.InputStream):
    """
    Run when All_Diets.csv is created or updated.

    This function:
    1. Reads the raw CSV file.
    2. Validates and cleans the dataset.
    3. Calculates nutritional ratios.
    4. Uploads Clean_Diets.csv to Blob Storage.
    5. Calculates the dashboard analysis.
    6. Stores the latest analysis in Cosmos DB.
    """
    logging.info(
        "CleanAndCache started. Blob name: %s, size: %s bytes",
        myblob.name,
        myblob.length,
    )

    # Read the blob once and reuse the bytes.
    raw_bytes = myblob.read()

    # Generate a hash to identify this version of the CSV.
    source_hash = hashlib.md5(raw_bytes).hexdigest()

    # Load the CSV file into a Pandas DataFrame.
    df = pd.read_csv(BytesIO(raw_bytes))

    # These columns are required by the analysis.
    required_columns = [
        "Diet_type",
        "Recipe_name",
        "Cuisine_type",
        "Protein(g)",
        "Carbs(g)",
        "Fat(g)",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"CSV is missing required columns: {missing_columns}"
        )

    numeric_columns = [
        "Protein(g)",
        "Carbs(g)",
        "Fat(g)",
    ]

    # Convert macro columns to numeric values.
    # Invalid values become NaN and are filled afterward.
    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    # Replace missing numeric values with the column average.
    df[numeric_columns] = df[numeric_columns].fillna(
        df[numeric_columns].mean()
    )

    # Replace missing text values with a safe default.
    df["Diet_type"] = df["Diet_type"].fillna("Unknown")
    df["Recipe_name"] = df["Recipe_name"].fillna("Unknown")
    df["Cuisine_type"] = df["Cuisine_type"].fillna(
        "Unknown"
    )

    # Calculate nutritional ratios.
    df["Protein_to_Carbs_ratio"] = (
        df["Protein(g)"] / df["Carbs(g)"]
    )

    df["Carbs_to_Fats_ratio"] = (
        df["Carbs(g)"] / df["Fat(g)"]
    )

    # Division by zero may produce positive or negative infinity.
    df.replace(
        [float("inf"), float("-inf")],
        0,
        inplace=True,
    )

    # Replace any remaining NaN values with zero.
    df.fillna(0, inplace=True)

    # Upload the cleaned dataset back to Blob Storage.
    blob_service = get_blob_service()

    clean_blob_client = blob_service.get_blob_client(
        container=CONTAINER_NAME,
        blob=CLEAN_BLOB_NAME,
    )

    clean_blob_client.upload_blob(
        df.to_csv(index=False),
        overwrite=True,
    )

    logging.info(
        "Clean_Diets.csv uploaded successfully."
    )

    # Calculate the average macros for each diet type.
    avg_macros = (
        df.groupby("Diet_type")[
            [
                "Protein(g)",
                "Carbs(g)",
                "Fat(g)",
            ]
        ]
        .mean()
        .reset_index()
    )

    # Select the five recipes with the most protein
    # from each diet type.
    top_protein = (
        df.sort_values(
            "Protein(g)",
            ascending=False,
        )
        .groupby("Diet_type")
        .head(5)[
            [
                "Diet_type",
                "Recipe_name",
                "Protein(g)",
            ]
        ]
    )

    # Find the most common cuisine for each diet type.
    common_cuisine = (
        df.groupby("Diet_type")["Cuisine_type"]
        .agg(
            lambda values: (
                values.mode().iloc[0]
                if not values.mode().empty
                else "Unknown"
            )
        )
        .reset_index()
    )

    # Calculate the average nutritional ratios
    # for each diet type.
    avg_ratios = (
        df.groupby("Diet_type")[
            [
                "Protein_to_Carbs_ratio",
                "Carbs_to_Fats_ratio",
            ]
        ]
        .mean()
        .reset_index()
    )

    # Create the single latest cache document.
    cache_document = {
        "id": "latest",
        "avg_macros": avg_macros.to_dict(
            orient="records"
        ),
        "top_protein": top_protein.to_dict(
            orient="records"
        ),
        "common_cuisine": common_cuisine.to_dict(
            orient="records"
        ),
        "avg_ratios": avg_ratios.to_dict(
            orient="records"
        ),
        "row_count": int(len(df)),
        "computed_at": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "source_hash": source_hash,
    }

    # Save or overwrite the latest analysis in Cosmos DB.
    cache_container = get_cosmos_container(
        CACHE_CONTAINER_NAME
    )

    cache_container.upsert_item(cache_document)

    logging.info(
        "CleanAndCache completed. Processed %s rows.",
        len(df),
    )


@app.route(
    route="GetDietAnalysis",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def GetDietAnalysis(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """
    Return the precomputed diet analysis from Cosmos DB.

    Optional query parameter:
        diet_type: Filter analysis by diet type.
    """
    start_time = time.time()

    try:
        cache_container = get_cosmos_container(
            CACHE_CONTAINER_NAME
        )

        try:
            document = cache_container.read_item(
                item="latest",
                partition_key="latest",
            )
        except exceptions.CosmosResourceNotFoundError:
            return json_response(
                {
                    "error": (
                        "No cached analysis yet. "
                        "Upload All_Diets.csv first."
                    )
                },
                404,
            )

        diet_filter = req.params.get("diet_type")

        # Return only application data and exclude Cosmos metadata.
        response_data = {
            "avg_macros": document.get(
                "avg_macros",
                [],
            ),
            "top_protein": document.get(
                "top_protein",
                [],
            ),
            "common_cuisine": document.get(
                "common_cuisine",
                [],
            ),
            "avg_ratios": document.get(
                "avg_ratios",
                [],
            ),
            "row_count": document.get(
                "row_count",
                0,
            ),
            "computed_at": document.get("computed_at"),
            "source_hash": document.get("source_hash"),
        }

        # Apply the optional diet type filter.
        if diet_filter:
            normalized_filter = (
                diet_filter.strip().lower()
            )

            analysis_fields = [
                "avg_macros",
                "top_protein",
                "common_cuisine",
                "avg_ratios",
            ]

            for field in analysis_fields:
                response_data[field] = [
                    row
                    for row in response_data[field]
                    if str(
                        row.get("Diet_type", "")
                    ).lower()
                    == normalized_filter
                ]

        response_data["execution_time_seconds"] = round(
            time.time() - start_time,
            4,
        )

        response_data["served_from"] = "cache"

        return json_response(response_data)

    except Exception as error:
        logging.exception("GetDietAnalysis failed")

        return json_response(
            {"error": str(error)},
            500,
        )


@app.route(
    route="GetRecipes",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def GetRecipes(req: func.HttpRequest) -> func.HttpResponse:
    """
    Return recipes from Clean_Diets.csv.

    Supported query parameters:
        diet_type: Exact diet type filter.
        keyword: Case-insensitive recipe name search.
        page: Requested page number.
        page_size: Number of recipes per page.
    """
    try:
        diet_type = req.params.get(
            "diet_type",
            "",
        ).strip()

        keyword = req.params.get(
            "keyword",
            "",
        ).strip()

        # Validate pagination parameters.
        try:
            page = int(
                req.params.get("page", "1")
            )

            page_size = int(
                req.params.get("page_size", "10")
            )
        except ValueError:
            return json_response(
                {
                    "error": (
                        "page and page_size must be integers"
                    )
                },
                400,
            )

        if page < 1:
            return json_response(
                {"error": "page must be at least 1"},
                400,
            )

        if page_size < 1 or page_size > 100:
            return json_response(
                {
                    "error": (
                        "page_size must be between "
                        "1 and 100"
                    )
                },
                400,
            )

        # Download the cleaned dataset.
        blob_service = get_blob_service()

        blob_client = blob_service.get_blob_client(
            container=CONTAINER_NAME,
            blob=CLEAN_BLOB_NAME,
        )

        clean_bytes = (
            blob_client.download_blob().readall()
        )

        df = pd.read_csv(BytesIO(clean_bytes))

        # Apply an exact, case-insensitive diet filter.
        if diet_type:
            df = df[
                df["Diet_type"]
                .astype(str)
                .str.lower()
                == diet_type.lower()
            ]

        # Apply a case-insensitive recipe name search.
        if keyword:
            df = df[
                df["Recipe_name"]
                .astype(str)
                .str.contains(
                    keyword,
                    case=False,
                    na=False,
                    regex=False,
                )
            ]

        total_results = int(len(df))

        total_pages = (
            math.ceil(total_results / page_size)
            if total_results > 0
            else 0
        )

        start_index = (page - 1) * page_size

        page_df = df.iloc[
            start_index:start_index + page_size
        ]

        recipe_columns = [
            "Diet_type",
            "Recipe_name",
            "Cuisine_type",
            "Protein(g)",
            "Carbs(g)",
            "Fat(g)",
        ]

        result = {
            "page": page,
            "page_size": page_size,
            "total_results": total_results,
            "total_pages": total_pages,
            "recipes": page_df[
                recipe_columns
            ].to_dict(orient="records"),
        }

        return json_response(result)

    except Exception as error:
        logging.exception("GetRecipes failed")

        return json_response(
            {"error": str(error)},
            500,
        )


@app.route(
    route="Register",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def Register(req: func.HttpRequest) -> func.HttpResponse:
    """
    Register a user with email and password.

    The plaintext password is never stored.
    Only the bcrypt password hash is saved in Cosmos DB.
    """
    try:
        try:
            body = req.get_json()
        except ValueError:
            return json_response(
                {
                    "error": (
                        "Request body must be valid JSON"
                    )
                },
                400,
            )

        email = str(
            body.get("email", "")
        ).strip().lower()

        password = str(
            body.get("password", "")
        )

        name = str(
            body.get("name", "")
        ).strip()

        if not email or "@" not in email:
            return json_response(
                {"error": "A valid email is required"},
                400,
            )

        if len(password) < 8:
            return json_response(
                {
                    "error": (
                        "Password must contain at least "
                        "8 characters"
                    )
                },
                400,
            )

        users_container = get_cosmos_container(
            USERS_CONTAINER_NAME
        )

        # Search for an existing account with the same email.
        existing_users = list(
            users_container.query_items(
                query=(
                    "SELECT c.id FROM c "
                    "WHERE c.email = @email"
                ),
                parameters=[
                    {
                        "name": "@email",
                        "value": email,
                    }
                ],
                enable_cross_partition_query=True,
            )
        )

        if existing_users:
            return json_response(
                {"error": "Email already registered"},
                409,
            )

        # Hash and salt the password using bcrypt.
        password_hash = bcrypt.hashpw(
            password.encode("utf-8"),
            bcrypt.gensalt(),
        ).decode("utf-8")

        user = {
            "id": str(uuid.uuid4()),
            "email": email,
            "name": name,
            "password_hash": password_hash,
            "provider": "password",
            "created_at": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
        }

        users_container.create_item(user)

        return json_response(
            {"message": "Registered successfully"},
            201,
        )

    except Exception as error:
        logging.exception("Register failed")

        return json_response(
            {"error": str(error)},
            500,
        )


@app.route(
    route="Login",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def Login(req: func.HttpRequest) -> func.HttpResponse:
    """
    Authenticate a user and return a signed JWT.

    The JWT expires after eight hours.
    """
    try:
        try:
            body = req.get_json()
        except ValueError:
            return json_response(
                {
                    "error": (
                        "Request body must be valid JSON"
                    )
                },
                400,
            )

        email = str(
            body.get("email", "")
        ).strip().lower()

        password = str(
            body.get("password", "")
        )

        if not email or not password:
            return json_response(
                {
                    "error": (
                        "Email and password are required"
                    )
                },
                400,
            )

        users_container = get_cosmos_container(
            USERS_CONTAINER_NAME
        )

        # Find the user by email address.
        matches = list(
            users_container.query_items(
                query=(
                    "SELECT * FROM c "
                    "WHERE c.email = @email"
                ),
                parameters=[
                    {
                        "name": "@email",
                        "value": email,
                    }
                ],
                enable_cross_partition_query=True,
            )
        )

        if not matches:
            return json_response(
                {"error": "Invalid email or password"},
                401,
            )

        user = matches[0]

        # Compare the supplied password with the stored hash.
        password_is_valid = bcrypt.checkpw(
            password.encode("utf-8"),
            user["password_hash"].encode("utf-8"),
        )

        if not password_is_valid:
            return json_response(
                {"error": "Invalid email or password"},
                401,
            )

        if not JWT_SECRET:
            raise ValueError(
                "JWT_SECRET is not configured"
            )

        # Generate a JWT that is valid for eight hours.
        token = jwt.encode(
            {
                "sub": user["id"],
                "email": user["email"],
                "name": user.get("name", ""),
                "exp": datetime.datetime.now(
                    datetime.timezone.utc
                )
                + datetime.timedelta(hours=8),
            },
            JWT_SECRET,
            algorithm="HS256",
        )

        return json_response(
            {
                "token": token,
                "name": user.get("name", ""),
                "email": user["email"],
            }
        )

    except Exception as error:
        logging.exception("Login failed")

        return json_response(
            {"error": str(error)},
            500,
        )


@app.route(
    route="health",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def health(req: func.HttpRequest) -> func.HttpResponse:
    """
    Check whether the backend can connect to Cosmos DB.
    """
    try:
        cache_container = get_cosmos_container(
            CACHE_CONTAINER_NAME
        )

        # Read the container properties to verify connectivity.
        cache_container.read()

        return json_response(
            {
                "status": "healthy",
                "cosmos": "connected",
                "storage": "configured",
            }
        )

    except Exception as error:
        logging.exception("Health check failed")

        return json_response(
            {
                "status": "unhealthy",
                "error": str(error),
            },
            500,
        )