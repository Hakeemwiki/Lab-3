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


# Function to infer file type based on the key
def infer_file_type(key):
    if 'songs' in key: return 'songs'
    elif 'streams' in key: return 'streams'
    elif 'users' in key: return 'users'
    return None


# File validation logic to check schema compliance and move valid/invalid files in s3
def validate_file(key, invalid_entries):
    file_type = infer_file_type(key)
    if not file_type:
        logger.warning(f"Skipping unrecognized file: {key}")
        return

    # Check if the file type has a required schema
    expected = REQUIRED_SCHEMAS[file_type]
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_csv(BytesIO(obj['Body'].read()))

    # Check if all expected columns are present
    missing = [col for col in expected if col not in df.columns]
    if missing:
        logger.error(f"{key} is missing: {missing}")
        s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': key}, Key=LOG_PREFIX + os.path.basename(key))
        s3.delete_object(Bucket=BUCKET, Key=key)
        invalid_entries.append(f"{key} missing: {missing}")
    else:
        validated_key = VALIDATED_PREFIX + os.path.basename(key)
        archive_key = ARCHIVE_PREFIX + os.path.basename(key)
        s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': key}, Key=validated_key)
        s3.copy_object(Bucket=BUCKET, CopySource={'Bucket': BUCKET, 'Key': key}, Key=archive_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        logger.info(f"{key} passed validation and moved to {validated_key} and archived to {archive_key}")

# function for main validation runner to process incoming s3 files and generate a report for schema violations
def main():
    logger.info("Starting validation of incoming files")
    # Ensure the log directory exists
    files = list_files()
    if not files:
        logger.warning("No incoming files to validate")
        return
    # Create the log directory if it doesn't exist
    invalid_entries = []
    for key in files:
        validate_file(key, invalid_entries)
    # Generate a report of invalid entries
    if invalid_entries:
        report_body = '\n'.join(invalid_entries)
        s3.put_object(Bucket=BUCKET, Key=LOG_PREFIX + 'report.txt', Body=report_body.encode('utf-8'))
        logger.warning("Validation report written to logs/invalid/report.txt")
    else:
        logger.info("All incoming files passed schema validation.")
