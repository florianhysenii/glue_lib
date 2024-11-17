from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc
from ingestion import DataIngestion
from processing import DataProcessor
from config import MYSQL_CONFIG, AWS_CONFIG

def aggregate_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("MovieLensAggregation").getOrCreate()

    s3_input_path = "output/movies/movie_data_processed/"  # Path to the processed data in S3
    data_ingestion = DataIngestion(spark, MYSQL_CONFIG)
    df = data_ingestion.read_csv(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_input_path}")

    # i. Calculate average rating for each movie
    avg_rating_df = df.groupBy("movieId").agg(avg("rating").alias("average_rating"))

    # ii. Find the most active users (highest number of ratings)
    active_users_df = df.groupBy("userId").agg(count("rating").alias("ratings_count"))
    most_active_users_df = active_users_df.orderBy(desc("ratings_count"))

    # iii. Additional analysis: Find the most highly-rated movie
    most_highly_rated_df = avg_rating_df.orderBy(desc("average_rating")).limit(1)

    # Step 3: Write aggregated results to S3 as Parquet
    s3_output_path = "output/movie_data_aggregated/"
    avg_rating_df.write.parquet(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_output_path}/avg_rating", mode="overwrite")
    most_active_users_df.write.parquet(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_output_path}/most_active_users", mode="overwrite")
    most_highly_rated_df.write.parquet(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_output_path}/most_highly_rated_movie", mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    aggregate_data()
