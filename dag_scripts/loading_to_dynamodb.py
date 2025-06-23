# import necessary libraries
import boto3
import pandas as pd
import os
from io import BytesIO
import logging
import math
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 and DynamoDB clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

# Define constants for S3 bucket and prefixes
BUCKET = 'music-stream-data-dynamo'
FILES = {
    'genre_metrics': 'curated/genre_kpis.csv/',
    'top_3_songs': 'curated/top_3_songs.csv/',
    'top_5_genres': 'curated/top_5_genres.csv/'
}

# Ensure tables create if they do not exist
def ensure_table_exists(name, schema):
    # Check if the table already exists
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if name not in existing_tables:
        logger.info(f"Creating DynamoDB table: {name}")
        dynamodb_client.create_table(
            TableName=name,
            KeySchema=schema['KeySchema'], # Define the primary key schema
            AttributeDefinitions=schema['AttributeDefinitions'], # Define the attribute definitions
            BillingMode='PAY_PER_REQUEST', # Use on-demand billing mode
            StreamSpecification={
                'StreamEnabled': True, # Enable streaming
                'StreamViewType': 'NEW_AND_OLD_IMAGES' # Stream both new and old images
            }
        )
        # Wait for the table to be created
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=name)
        logger.info(f"Table {name} is ready.")

def sanitize_value(v):
    """
    Sanitizes a given value 'v' for data processing, handling NaN/null values,
    converting floats to integers, and ensuring all other values are strings.

    Args:
        v: The input value to be sanitized. It can be of various types,
           including pandas NaN, float, int, or string.

    Returns:
        None if the input value is null or NaN.
        An integer if the input value is a float (after converting it to an int).
        A string representation for all other non-null, non-NaN values.
    """
    if pd.isnull(v) or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, float):
        return int(v)
    return str(v)

def extract_partition_date(key):
    """
    Extracts the date from the S3 key if it follows the pattern 'date=YYYY-MM-DD/'."""
    match = re.search(r"date=([\d\-]+)/", key)
    if match:
        return match.group(1)
    return None
