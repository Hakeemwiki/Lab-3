# Real-Time Music Streaming Analytics Pipeline on AWS

Purpose: Implement a scalable, event-driven ETL pipeline on AWS to process music streaming data from S3, validate it, compute daily genre-level KPIs, and store results in DynamoDB for real-time analytics and application consumption.

---

## Table of Contents

1. [Problem Statement](#-problem-statement)
2. [Objectives](#-objectives)
3. [Datasets](#-datasets)
4. [Architecture Overview](#-architecture-overview)
5. [Tech Stack](#-tech-stack)
6. [Pipeline Workflow](#-pipeline-workflow)
7. [Computed KPIs](#-computed-kpis)
8. [Setup & Deployment](#-setup--deployment)
9. [IAM Policies](#-iam-policies)
10. [Visuals](#-visuals)
11. [Folder Structure](#-folder-structure)
13. [Future Improvements](#-future-improvements)
14. [Lessons Learned](#-lessons-learned)
15. [Contributors](#-contributors)

---

## Problem Statement

A music streaming service generates large volumes of user streaming data in batch CSV files, which arrive unpredictably in **Amazon S3**. These files contain critical information about user behavior, song popularity, and genre trends. Stakeholders require real-time insights to:

- Track **daily genre-level KPIs** (e.g., listen counts, unique listeners, listening time).
- Identify **top songs and genres** to inform recommendations and marketing.
- Enable **fast data access** for downstream applications via a NoSQL database.
- Ensure **data quality** through robust validation before processing.

The challenge is to build a **real-time, event-driven, and scalable** data pipeline that ingests, validates, transforms, and stores data efficiently while maintaining automation, reliability, and cost-effectiveness.

---

## Objectives

The pipeline aims to:

1. Ingest streaming batch data from **Amazon S3** at irregular intervals.
2. Validate incoming data for schema consistency and completeness.
3. Transform data using **AWS Glue** and **Apache Spark** to compute daily KPIs.
4. Store processed metrics in **Amazon DynamoDB** for low-latency access.
5. Orchestrate the pipeline using **Apache Airflow (MWAA)** for automation and reliability.
6. Archive processed files and log errors for traceability.
7. Provide clear documentation for setup, execution, and querying.

---

## Datasets

Raw data is stored in Amazon S3 under the prefix `s3://music-streaming-data/incoming/` in CSV format. The datasets include:

- **`streams.csv`**:
  - Columns: `stream_id`, `user_id`, `song_id`, `genre`, `timestamp`, `duration_ms`
  - Description: Records of individual streaming events with user, song, and duration details.
- **`songs.csv`**:
  - Columns: `song_id`, `title`, `artist`, `genre`, `release_date`
  - Description: Metadata for songs available on the platform.
- **`users.csv`**:
  - Columns: `user_id`, `first_name`, `last_name`, `email`, `signup_date`
  - Description: User profile information.

**Example Schema Preview**:

| Dataset       | Sample Columns                          | Format | S3 Path                              |
|---------------|-----------------------------------------|--------|--------------------------------------|
| streams.csv   | stream_id, user_id, song_id, genre      | CSV    | s3://music-streaming-data/incoming/streams/ |
| songs.csv     | song_id, title, genre                   | CSV    | s3://music-streaming-data/incoming/songs/   |
| users.csv     | user_id, email, signup_date             | CSV    | s3://music-streaming-data/incoming/users/   |

---

## Architecture Overview

The pipeline leverages AWS services to create a modular, serverless, and scalable ETL workflow.

### Architecture Diagram

![alt text](docs/lab3-Mwaa.drawio.svg)

### Key Components:

- **Amazon S3**:
  - Stores raw data (`incoming/`), validated data (`validated/`), curated KPIs (`curated/`), archived files (`archive/`), and error logs (`logs/invalid/`).
  - Uses partitioning by date for optimized storage and querying.
- **AWS Glue**:
  - **Python Shell Jobs**: Validate incoming data and load results to DynamoDB.
  - **Spark Jobs**: Transform data and compute KPIs using Apache Spark.
- **Apache Airflow (MWAA)**:
  - Orchestrates the pipeline using a Directed Acyclic Graph (DAG).
  - Uses `S3KeySensor` or **Amazon EventBridge** to detect new files.
- **Amazon DynamoDB**:
  - Stores KPIs in three tables optimized for low-latency queries.
- **Amazon CloudWatch**:
  - Logs pipeline execution and errors for monitoring and debugging.

**Flow Overview**:
1. New CSV files arrive in `s3://music-streaming-data/incoming/`.
2. Airflow detects files via `S3KeySensor` or EventBridge.
3. A Glue Python Shell job validates file schemas and moves valid files to `validated/`.
4. A Glue Spark job processes validated data to compute KPIs.
5. KPIs are written as partitioned CSVs to `curated/`.
6. A Glue Python Shell job loads KPIs into DynamoDB.
7. Processed files are archived to `archive/`.

---

## Tech Stack

| Service/Component           | Purpose                                      |
|----------------------------|----------------------------------------------|
| **Amazon S3**              | Storage for raw, validated, curated, and archived data |
| **AWS Glue (PySpark)**     | Data transformation and KPI computation      |
| **AWS Glue (Python Shell)**| Data validation and DynamoDB ingestion       |
| **Apache Airflow (MWAA)**  | Pipeline orchestration and scheduling        |
| **Amazon DynamoDB**        | NoSQL storage for low-latency KPI access     |
| **Amazon EventBridge**     | Optional event-driven trigger for S3 uploads |
| **Amazon CloudWatch**      | Logging and monitoring                      |
| **Boto3 SDK**              | Programmatic AWS service interactions        |

---

## 🛠️ Pipeline Workflow

### Step 1: Data Ingestion
- Raw CSV files are uploaded to `s3://music-streaming-data/incoming/` (e.g., `streams/2025-06-23/data.csv`).
- Airflow’s `S3KeySensor` or EventBridge triggers the pipeline on new file detection.

### Step 2: Data Validation
- **Glue Job**: `validate_streaming_data` (Python Shell)
- **Purpose**: Ensures required columns (`stream_id`, `user_id`, `song_id`, `genre`, `timestamp`, `duration_ms`) are present and valid.
- **Logic**:
  - Uses Pandas to read CSV files.
  - Checks for missing columns, null values, and data type consistency.
  - Valid files are copied to `s3://music-streaming-data/validated/`.
  - Invalid files are moved to `s3://music-streaming-data/logs/invalid/` with error logs.
- **Output**: Validated files ready for transformation.

**Sample Validation Code** (from `validate_streaming_data.py`):
```python
import pandas as pd
import boto3

s3_client = boto3.client('s3')
required_columns = ['stream_id', 'user_id', 'song_id', 'genre', 'timestamp', 'duration_ms']

def validate_file(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        s3_client.copy_object(Bucket=bucket, Key=f"logs/invalid/{key.split('/')[-1]}", CopySource={'Bucket': bucket, 'Key': key})
        s3_client.delete_object(Bucket=bucket, Key=key)
        raise ValueError(f"Missing columns: {missing_cols}")
    s3_client.copy_object(Bucket=bucket, Key=f"validated/{key.split('/')[-1]}", CopySource={'Bucket': bucket, 'Key': key})
    return True
```

### Step 3: Data Transformation
- **Glue Job**: `transform_streaming_metrics` (PySpark)
- **Purpose**: Computes daily genre-level KPIs and top rankings.
- **Logic**:
  - Reads validated CSVs from `s3://music-streaming-data/validated/`.
  - Joins `streams.csv` with `songs.csv` for genre information.
  - Aggregates data by `genre` and `date` (derived from `timestamp`).
  - Computes KPIs: `total_streams`, `unique_listeners`, `total_listening_time`, `avg_listening_time_per_user`.
  - Identifies top 3 songs per genre and top 5 genres per day.
- **Output**: Partitioned CSVs in `s3://music-streaming-data/curated/` (e.g., `curated/date=2025-06-23/`).

**Sample Spark Code** (from `transform_streaming_metrics.py`):
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("StreamingMetrics").getOrCreate()

# Read validated data
streams = spark.read.csv("s3://music-streaming-data/validated/streams/", header=True)
songs = spark.read.csv("s3://music-streaming-data/validated/songs/", header=True)

# Compute genre-level KPIs
genre_metrics = streams.join(songs, "song_id") \
    .groupBy("genre", spark.sql("date_trunc('day', timestamp)").alias("date")) \
    .agg(
        count("*").alias("total_streams"),
        countDistinct("user_id").alias("unique_listeners"),
        sum("duration_ms").alias("total_listening_time"),
        avg("duration_ms").alias("avg_listening_time_per_user")
    )

# Top 3 songs per genre
window = Window.partitionBy("genre", "date").orderBy(col("total_streams").desc())
top_songs = streams.join(songs, "song_id") \
    .groupBy("genre", "date", "song_id", "title") \
    .agg(count("*").alias("total_streams")) \
    .withColumn("rank", rank().over(window)) \
    .filter(col("rank") <= 3)

# Top 5 genres
top_genres = genre_metrics.groupBy("date") \
    .agg(count("*").alias("total_streams")) \
    .orderBy(col("total_streams").desc()) \
    .limit(5)

# Write outputs
genre_metrics.write.partitionBy("date").csv("s3://music-streaming-data/curated/genre_metrics/")
top_songs.write.partitionBy("date").csv("s3://music-streaming-data/curated/top_songs/")
top_genres.write.partitionBy("date").csv("s3://music-streaming-data/curated/top_genres/")
```

### Step 4: DynamoDB Ingestion
- **Glue Job**: `load_to_dynamodb` (Python Shell)
- **Purpose**: Loads curated KPIs into DynamoDB tables.
- **Logic**:
  - Reads partitioned CSVs from `s3://music-streaming-data/curated/`.
  - Uses Boto3 to batch-write data to three DynamoDB tables:
    - `genre_metrics` (PK: `genre`, SK: `date`)
    - `top_3_songs` (PK: `genre`, SK: `date`)
    - `top_5_genres` (PK: `date`, SK: `genre`)
- **Output**: KPIs stored in DynamoDB for low-latency queries.

### Step 5: File Archival
- **Airflow Task**: Moves processed files from `incoming/` to `s3://music-streaming-data/archive/`.
- **Logic**: Uses Boto3 to copy and delete files after successful processing.

### Step 6: Airflow DAG Orchestration
- **DAG**: `music_streaming_etl_pipeline`
- **Tasks**:
  - `check_new_files`: `S3KeySensor` to detect new files in `incoming/`.
  - `validate_data`: Triggers `validate_streaming_data` Glue job.
  - `branch_after_validation`: Proceeds if validation succeeds; else logs error.
  - `transform_metrics`: Triggers `transform_streaming_metrics` Glue job.
  - `load_to_dynamodb`: Triggers `load_to_dynamodb` Glue job.
  - `archive_file`: Archives processed files.

**Sample DAG Code** (from `music_streaming_etl_dag.py`):
```python
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG('music_streaming_etl_pipeline', start_date=datetime(2025, 6, 23), schedule_interval=None) as dag:
    check_files = S3KeySensor(
        task_id='check_new_files',
        bucket_name='music-streaming-data',
        bucket_key='incoming/streams/*',
        aws_conn_id='aws_default'
    )
    
    validate = GlueJobOperator(
        task_id='validate_data',
        job_name='validate_streaming_data',
        aws_conn_id='aws_default'
    )
    
    transform = GlueJobOperator(
        task_id='transform_metrics',
        job_name='transform_streaming_metrics',
        aws_conn_id='aws_default'
    )
    
    load = GlueJobOperator(
        task_id='load_to_dynamodb',
        job_name='load_to_dynamodb',
        aws_conn_id='aws_default'
    )
    
    check_files >> validate >> transform >> load
```

---

## Computed KPIs

### Genre-Level KPIs (per day)
- **Total Streams**: Count of streams per genre.
- **Unique Listeners**: Distinct `user_id` count per genre.
- **Total Listening Time**: Sum of `duration_ms` per genre.
- **Average Listening Time per User**: Average `duration_ms` per `user_id`.

### Top Rankings
- **Top 3 Songs per Genre per Day**: Songs with the highest stream counts.
- **Top 5 Genres per Day**: Genres with the highest stream counts.

**DynamoDB Table Structures**:
| Table            | Partition Key | Sort Key | Attributes                              |
|------------------|---------------|----------|-----------------------------------------|
| genre_metrics    | genre         | date     | total_streams, unique_listeners, total_listening_time, avg_listening_time_per_user |
| top_3_songs      | genre         | date     | song_id, title, total_streams           |
| top_5_genres     | date          | genre    | total_streams                           |

---

## Setup & Deployment

### Prerequisites
- AWS account with access to S3, Glue, MWAA, DynamoDB, and CloudWatch.
- Configured IAM roles for Glue and MWAA with necessary permissions.
- Python 3.8+ for Glue Python Shell jobs.

### Step-by-Step Deployment
1. **Create S3 Buckets**:
   - Set up `s3://music-streaming-data/` with prefixes: `incoming/`, `validated/`, `curated/`, `archive/`, `logs/invalid/`.
2. **Configure DynamoDB Tables**:
   - Create `genre_metrics`, `top_3_songs`, and `top_5_genres` with appropriate keys.
   - Enable auto-scaling for read/write capacity.
3. **Upload Glue Scripts**:
   - Place `validate_streaming_data.py`, `transform_streaming_metrics.py`, and `load_to_dynamodb.py` in `s3://music-streaming-data/scripts/`.
4. **Set Up MWAA**:
   - Deploy `music_streaming_etl_dag.py` to the MWAA DAGs folder.
   - Configure `aws_default` connection in Airflow.
5. **Configure Glue Jobs**:
   - Create jobs in AWS Glue Console, linking to the uploaded scripts.
   - Assign appropriate IAM roles and S3 paths.
6. **Optional: Enable EventBridge**:
   - Set up an EventBridge rule to trigger the DAG on S3 uploads to `incoming/`.
7. **Test the Pipeline**:
   - Upload a sample CSV to `s3://music-streaming-data/incoming/streams/`.
   - Trigger the DAG manually or wait for the sensor/event trigger.
8. **Query DynamoDB**:
   - Use AWS Console or SDK to verify KPI data in DynamoDB tables.

**Sample DynamoDB Query**:
```python
import boto3

dynamodb = boto3.client('dynamodb')
response = dynamodb.query(
    TableName='genre_metrics',
    KeyConditionExpression='genre = :g and #dt = :d',
    ExpressionAttributeValues={':g': {'S': 'Pop'}, ':d': {'S': '2025-06-23'}},
    ExpressionAttributeNames={'#dt': 'date'}
)
print(response['Items'])
```

---

## 🔐 IAM Policies

The following IAM policies are required for Glue and MWAA:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::music-streaming-data/*",
        "arn:aws:s3:::music-streaming-data"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:BatchWriteItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/genre_metrics",
        "arn:aws:dynamodb:*:*:table/top_3_songs",
        "arn:aws:dynamodb:*:*:table/top_5_genres"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/glue/*"
    }
  ]
}
```

---

## Visuals
![alt text](docs/lab4-s3-bucket.png)
- *S3 bucket structure diagram*

![alt text](docs/lab3-dag-run.png)

![alt text](docs/lab3-skipped-dag-run.png)
- *Airflow DAG execution screenshot*

![alt text](docs/lab3-tablesDynamo.png)

![alt text](docs/lab3-genre-metrics.png)
- *DynamoDB table view with sample KPIs*

---

## 📂 Folder Structure

```
├── dags/
│   └── music_streaming_etl_dag.py
├── glue_jobs/
│   ├── validate_streaming_data.py
│   ├── transform_streaming_metrics.py
│   └── load_to_dynamodb.py
├── s3/
│   ├── incoming/
│   ├── validated/
│   ├── curated/
│   ├── archive/
│   └── logs/invalid/
├── README.md
```

---

## Future Improvements(Upon Research)

- Implement **Glue Job Bookmarking** to handle incremental data and avoid reprocessing.
- Deploy **CloudWatch Dashboards** for real-time KPI monitoring.
- Introduce **unit tests** for validation and transformation logic.
- Use **AWS CDK/Terraform** for infrastructure-as-code deployment.
- Enable **Amazon SNS** notifications for pipeline failures or SLA breaches.

---

## Lessons Learned

- **Event-driven triggers** (e.g., EventBridge) simplify real-time processing compared to polling with `S3KeySensor`.
- **Partitioning by date** in S3 and DynamoDB improves performance and scalability.
- **Schema validation** is critical to prevent downstream errors in real-time pipelines.
- **Airflow’s branching** enables flexible error handling and conditional execution.
- **DynamoDB key design** significantly impacts query performance and cost.

---

## Contributors

- **Hakeem** – Data Engineer & Architect
- Special thanks to the AWS and Airflow communities for documentation and best practices.

---
