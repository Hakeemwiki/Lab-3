# import necessary libraries
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime,timedelta
import boto3
import time
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow_dag")


def run_glue_job(job_name):
    """
    Starts an AWS Glue job and waits for its completion.

    This function initiates a specified AWS Glue job and then polls its status
    periodically until the job reaches a terminal state (SUCCEEDED, FAILED, or STOPPED).
    If the job does not succeed, it raises an exception.

    Args:
        job_name (str): The name of the AWS Glue job to be started.

    Raises:
        Exception: If the Glue job fails or is stopped.
    """
    # Initialize the Glue client
    glue_client = boto3.client('glue')
    logger.info(f"Starting Glue job: {job_name}")
    response = glue_client.start_job_run(JobName=job_name) # Start the Glue job
    job_run_id = response['JobRunId'] # Get the job run ID from the response
    logger.info(f"Started Glue job {job_name}, run ID: {job_run_id}")

    while True: # Poll for job status
        status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)['JobRun']['JobRunState'] # Get the current status of the job
        logger.info(f"Glue job {job_name} status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'STOPPED']: # Check if the job has reached a terminal state
            break # Exit the loop if the job is no longer running
        time.sleep(30) # Sleep for 30 seconds before checking the status again

    if status != 'SUCCEEDED': # If the job did not succeed, raise an exception
        raise Exception(f"Glue job {job_name} failed with status {status}")
    logger.info(f"Glue job {job_name} completed successfully.")


def archive_processed_files():
    # Archives processed CSV files from the S3 bucket.
    s3_client = boto3.client('s3')
    bucket = 'music-stream-data-dynamo' # S3 bucket name
    prefix = 'incoming/' # Prefix for incoming files
    archive_prefix = 'archive/' # Prefix for archived files

    logger.info("Archiving processed CSV files")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get('Contents', []): # List objects in the specified bucket and prefix
        key = obj['Key']
        if key.endswith('.csv'): # Check if the object is a CSV file
            archive_key = key.replace('incoming/', 'archive/') 
            s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=archive_key)
            s3_client.delete_object(Bucket=bucket, Key=key) # Delete the original file after copying it to the archive
            logger.info(f"Archived: {key}")

def check_validation():
    # Checks for validation errors in S3 and returns the next task based on the result.
    logger.info("Checking for validation errors in S3")
    s3 = boto3.client('s3')
    result = s3.list_objects_v2(Bucket='music-stream-data-dynamo', Prefix='logs/invalid/') # List objects in the S3 bucket under the 'logs/invalid/' prefix
    if 'Contents' in result: # If there are any objects in the 'logs/invalid/' prefix, it indicates validation errors
        issue_files = [obj['Key'] for obj in result['Contents'] if obj['Key'] != 'logs/invalid/' and not obj['Key'].endswith('/')]
        if issue_files: # If there are any issue files, log them and return 'end_pipeline'
            logger.warning(f"Validation failed: Found {len(issue_files)} issue file(s):")
            for key in issue_files:
                logger.warning(f" - {key}")
            return 'end_pipeline'
    logger.info("No validation errors found. Proceeding to transformation.")
    return 'transform_metrics'

default_args = {
    'owner': 'hakeem',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='music_streaming_etl_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes=15), # Changed schedule to every 15 minutes
    #schedule_interval=None,
    catchup=False,
    description='ETL pipeline for music streaming genre KPIs to DynamoDB',
)

