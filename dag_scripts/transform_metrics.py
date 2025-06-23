# import necessary libraries
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, sum as _sum, row_number, desc
from pyspark.sql.window import Window
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Storage constants
BUCKET = 'music-stream-data-dynamo'
SONGS_KEY = 'validated/songs.csv'
STREAMS_KEY = 'validated/streams*.csv'
USERS_KEY = 'validated/users.csv'

# Initialize Spark session
spark = SparkSession.builder.appName("TransformKPIs").getOrCreate()

# Read validated data from S3
logger.info("Reading validated data from S3")
songs_df = spark.read.option("header", True).csv(f"s3://{BUCKET}/{SONGS_KEY}")
streams_df = spark.read.option("header", True).csv(f"s3://{BUCKET}/{STREAMS_KEY}")
users_df = spark.read.option("header", True).csv(f"s3://{BUCKET}/{USERS_KEY}")

