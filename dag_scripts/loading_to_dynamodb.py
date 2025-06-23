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

