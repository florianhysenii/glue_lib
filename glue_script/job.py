from pyspark.sql import SparkSession
from ingestion import DataIngestion
from processing import DataProcessor
from config import MYSQL_CONFIG, AWS_CONFIG

def process_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("MovieLensETL").getOrCreate()

    s3_input_path = "input/movie_data.csv"  # Specify your S3 path
    data_ingestion = DataIngestion(spark, MYSQL_CONFIG)
    df = data_ingestion.read_csv(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_input_path}")

    data_processor = DataProcessor(df)
    df_cleaned = data_processor.clean_data()  # Cleaning data (drop duplicates and nulls)
    df_transformed = data_processor.transform_data()  # Add timestamp column

    # Step 3: Write transformed data to MySQL
    table_name = "movies"
    data_ingestion.write_to_mysql(df_transformed, table_name)

    s3_output_path = "output/movie_data_processed/"
    df_transformed.write.parquet(f"s3://{AWS_CONFIG['s3_bucket']}/{s3_output_path}", mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    process_data()
