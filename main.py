from ingestion import DataIngestion
from processing import DataProcessor
from config import MYSQL_CONFIG, AWS_CONFIG
from infra import GlueJobInfra
from pyspark.sql import SparkSession



# def run_pipeline():
#     # Step 1: CSV ingestion, processing, and saving to MySQL (already in ingestion.py and processing.py)
#     # Assuming this is already handled (refer to previous steps)

#     # Initialize Glue job infrastructure
#     glue_infra = GlueJobInfra(
#         glue_role_arn='arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole',
#         region=AWS_CONFIG['region'],
#         s3_bucket=AWS_CONFIG['s3_bucket'],
#         glue_script_path='scripts/your_glue_script.py',
#         glue_job_name='MovieLensETLJob'
#     )

#     glue_infra.upload_script_to_s3()

#     glue_job_name = glue_infra.create_glue_job()
#     job_run_id = glue_infra.start_glue_job()

#     status = glue_infra.check_glue_job_status(job_run_id)
#     print(f"Glue job {status}.")


# if __name__ == "__main__":
#     run_pipeline()


def run_local_pipeline():
    spark = SparkSession.builder.appName('abc').getOrCreate()
    ingestion = DataIngestion(spark, MYSQL_CONFIG)

    df = ingestion.read_csv("datasets\movie.csv")
    
    processor = DataProcessor(df)
    cleaned_data = processor.clean_data()
    transformed_data = processor.transform_data()

    ingestion.write_to_mysql(cleaned_data, "movies")

    transformed_data.write.parquet("path/to/output/movie_data_processed.parquet")

if __name__ == "__main__":
    run_local_pipeline()
