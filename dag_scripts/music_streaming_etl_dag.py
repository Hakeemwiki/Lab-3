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


