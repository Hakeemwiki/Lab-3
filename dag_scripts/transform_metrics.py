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

# Deduplicate data
songs_df = songs_df.dropDuplicates(["track_id"])
streams_df = streams_df.dropDuplicates(["user_id", "track_id", "listen_time"])
users_df = users_df.dropDuplicates(["user_id"])

# Extract date from listen_time
streams_df = streams_df.withColumn("date", to_date("listen_time"))

# Join all three datasets
joined_df = streams_df.join(songs_df, on="track_id", how="inner").join(users_df, on="user_id", how="left")
joined_df = joined_df.withColumn("duration_ms", col("duration_ms").cast("long"))


# Aggregate genre-level KPIs
genre_kpi_df = joined_df.groupBy("track_genre", "date").agg(
    countDistinct("user_id").alias("unique_listeners"),
    _sum("duration_ms").alias("total_listening_time"),
    countDistinct("track_id").alias("total_tracks"),
    countDistinct("listen_time").alias("total_streams")
)

# Calculate average listening time per user
genre_kpi_df = genre_kpi_df.withColumn(
    "avg_listening_time_per_user",
    (col("total_listening_time") / col("unique_listeners")).cast("long")
)

# Identify top 3 songs per genre per day
song_rank_window = Window.partitionBy("track_genre", "date").orderBy(desc("listen_time"))
ranked_songs = joined_df.withColumn("rank", row_number().over(song_rank_window))
top_3_songs_df = ranked_songs.filter(col("rank") <= 3).select(
    col("track_genre").alias("genre"), "date", "track_name", "rank"
)

# Identify top 5 genres per day
genre_rank_window = Window.partitionBy("date").orderBy(desc("total_streams"))
top_genres_df = genre_kpi_df.withColumn("genre_rank", row_number().over(genre_rank_window))
top_5_genres_df = top_genres_df.filter(col("genre_rank") <= 5).select(
    col("track_genre").alias("genre"), "date", "total_streams", "genre_rank"
)