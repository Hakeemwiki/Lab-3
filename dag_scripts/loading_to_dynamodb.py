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
