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


def load_csv_to_dynamodb(table_name, prefix):
    logger.info(f"Searching for CSVs in {prefix} on S3")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    if 'Contents' not in response:
        logger.warning(f"No files found under {prefix}, skipping.")
        return

    # Filter for CSV files and part files
    csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv') or 'part' in obj['Key']]
    if not csv_files:
        logger.warning(f"No part files found in {prefix}, skipping.")
        return

    for csv_key in sorted(csv_files):
        logger.info(f"Reading {csv_key} from S3")
        response = s3.get_object(Bucket=BUCKET, Key=csv_key) # Get the CSV file from S3
        df = pd.read_csv(response['Body'])

        # Extract partitioned date from path if missing in DataFrame
        if 'date' not in df.columns:
            extracted_date = extract_partition_date(csv_key) # Extract date from the S3 key
            if extracted_date:
                df['date'] = extracted_date # Add the extracted date as a new column
            else:
                logger.error("Missing 'date' column and could not extract from path. Skipping file.")
                continue

        df.drop_duplicates(inplace=True)
        table = dynamodb.Table(table_name) # Get the DynamoDB table object
        logger.info(f"Inserting into {table_name}")

        for _, row in df.iterrows(): # Iterate over each row in the DataFrame
            item = {k: sanitize_value(v) for k, v in row.items() if sanitize_value(v) is not None} # Sanitize each value
            table.put_item(Item=item) # Insert the sanitized item into DynamoDB

        logger.info(f"Loaded {len(df)} records into {table_name}")


def main(): # Main function to load data into DynamoDB
    table_schemas = {
        'genre_metrics': {
            'KeySchema': [
                {'AttributeName': 'genre', 'KeyType': 'HASH'},
                {'AttributeName': 'date', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'genre', 'AttributeType': 'S'},
                {'AttributeName': 'date', 'AttributeType': 'S'}
            ]
        },
        'top_3_songs': {
            'KeySchema': [
                {'AttributeName': 'genre', 'KeyType': 'HASH'},
                {'AttributeName': 'date', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'genre', 'AttributeType': 'S'},
                {'AttributeName': 'date', 'AttributeType': 'S'}
            ]
        },
        'top_5_genres': {
            'KeySchema': [
                {'AttributeName': 'date', 'KeyType': 'HASH'},
                {'AttributeName': 'genre', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'date', 'AttributeType': 'S'},
                {'AttributeName': 'genre', 'AttributeType': 'S'}
            ]
        }
    }

    for table_name, s3_prefix in FILES.items(): # Iterate over each table and its corresponding S3 prefix
        ensure_table_exists(table_name, table_schemas[table_name]) # Ensure the table exists
        load_csv_to_dynamodb(table_name, s3_prefix) # Load the CSV data into DynamoDB

if __name__ == "__main__":
    logger.info("Starting DynamoDB loading script")
    main()
    logger.info("DynamoDB loading completed")