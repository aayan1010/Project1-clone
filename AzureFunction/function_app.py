import datetime
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
import requests
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from azure.storage.blob import BlobServiceClient


# Initialize the Azure Functions application.
#
# IMPORTANT: This app is linked to a Static Web App via the managed
# Functions integration, which only supports HTTP-triggered functions.
# The Blob-triggered cleaning function has been moved to a separate,
# standalone Function App (see BlobCleanerFunction/) that is deployed
# independently.
app = func.FunctionApp()


# Blob Storage configuration.
CONTAINER_NAME = "diet-analysis"
CLEAN_BLOB_NAME = "Clean_Diets.csv"

# Cosmos DB configuration.
DATABASE_NAME = "DietAnalysisDB"
CACHE_CONTAINER_NAME = "Cache"
USERS_CONTAINER_NAME = "Users"

# JWT secret is loaded from local.settings.json or Azure App Settings.
JWT_SECRET = os.environ.get("JWT_SECRET", "")

# GitHub OAuth configuration. The Client Secret is never hardcoded here —
# both values are read from Azure App Settings (Environment variables)
# at runtime, the same way COSMOS_CONNECTION_STRING is loaded above.
GITHUB_CLIENT_ID = os.environ.get("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.environ.get("GITHUB_CLIENT_SECRET", "")


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
    route="GitHubCallback",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def GitHubCallback(req: func.HttpRequest) -> func.HttpResponse:
    """
    Handle the GitHub OAuth redirect.

    GitHub sends the user back here with a temporary `code` after
    they approve access. This function exchanges that code for an
    access token, fetches the user's GitHub profile, creates or
    finds a matching user in Cosmos DB, and returns a signed JWT
    the same way the password-based Login route does.
    """
    try:
        if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
            raise ValueError(
                "GITHUB_CLIENT_ID / GITHUB_CLIENT_SECRET "
                "are not configured"
            )

        code = req.params.get("code")

        if not code:
            return json_response(
                {"error": "Missing 'code' parameter"},
                400,
            )

        # Exchange the temporary code for an access token.
        # This call must happen server-side — it's the one place
        # the Client Secret is ever used, and it never leaves
        # this function.
        token_response = requests.post(
            "https://github.com/login/oauth/access_token",
            headers={"Accept": "application/json"},
            data={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": code,
            },
            timeout=10,
        )

        token_response.raise_for_status()
        token_data = token_response.json()

        access_token = token_data.get("access_token")

        if not access_token:
            return json_response(
                {
                    "error": token_data.get(
                        "error_description",
                        "Failed to obtain access token from GitHub",
                    )
                },
                401,
            )

        # Fetch the authenticated user's GitHub profile.
        profile_response = requests.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=10,
        )

        profile_response.raise_for_status()
        profile = profile_response.json()

        github_id = str(profile.get("id", ""))
        name = profile.get("name") or profile.get("login", "")
        email = profile.get("email")

        # GitHub only returns a public email if the user has one set.
        # Fall back to the emails endpoint to find a primary email.
        if not email:
            emails_response = requests.get(
                "https://api.github.com/user/emails",
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Accept": "application/vnd.github+json",
                },
                timeout=10,
            )

            if emails_response.ok:
                for entry in emails_response.json():
                    if entry.get("primary"):
                        email = entry.get("email")
                        break

        if not email:
            email = f"{github_id}@users.noreply.github.com"

        email = email.strip().lower()

        users_container = get_cosmos_container(
            USERS_CONTAINER_NAME
        )

        # Look for an existing account linked to this GitHub ID.
        existing_users = list(
            users_container.query_items(
                query=(
                    "SELECT * FROM c "
                    "WHERE c.github_id = @github_id"
                ),
                parameters=[
                    {
                        "name": "@github_id",
                        "value": github_id,
                    }
                ],
                enable_cross_partition_query=True,
            )
        )

        if existing_users:
            user = existing_users[0]
        else:
            user = {
                "id": str(uuid.uuid4()),
                "email": email,
                "name": name,
                "github_id": github_id,
                "provider": "github",
                "created_at": datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat(),
            }

            users_container.create_item(user)

        if not JWT_SECRET:
            raise ValueError(
                "JWT_SECRET is not configured"
            )

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

    except requests.RequestException as error:
        logging.exception("GitHub OAuth request failed")

        return json_response(
            {"error": f"GitHub request failed: {error}"},
            502,
        )

    except Exception as error:
        logging.exception("GitHubCallback failed")

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