import datetime
import hashlib
import logging
import os
from io import BytesIO

import azure.functions as func
import pandas as pd
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient


# Initialize the Azure Functions application.
#
# This app is deployed on its own (not through the Static Web Apps
# managed integration) because Static Web Apps only supports
# HTTP-triggered functions. This app exists solely to run
# CleanAndCache whenever All_Diets.csv changes in Blob Storage.
app = func.FunctionApp()


# Blob Storage configuration.
CONTAINER_NAME = "diet-analysis"
RAW_BLOB_NAME = "All_Diets.csv"
CLEAN_BLOB_NAME = "Clean_Diets.csv"

# Cosmos DB configuration.
DATABASE_NAME = "DietAnalysisDB"
CACHE_CONTAINER_NAME = "Cache"


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