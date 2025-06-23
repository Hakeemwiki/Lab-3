# import necessary libraries
import boto3
import pandas as pd
import os
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3 = boto3.client('s3')

# Define constants for S3 bucket and prefixes
BUCKET = 'music-stream-data-dynamo'
INPUT_PREFIX = 'incoming/'
LOG_PREFIX = 'logs/invalid/'
VALIDATED_PREFIX = 'validated/'
ARCHIVE_PREFIX = 'archive/'

# dictionary to hold the required schemas for each table
REQUIRED_SCHEMAS = {
    'songs': ['track_id', 'artists', 'album_name', 'track_name', 'popularity', 'duration_ms', 'explicit',
              'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
              'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature', 'track_genre'],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}


# Function to list incoming files with input prefix
def list_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=INPUT_PREFIX)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]



