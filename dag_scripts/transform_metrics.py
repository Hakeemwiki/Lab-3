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



