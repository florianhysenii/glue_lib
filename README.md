# MovieLens Data Processing Pipeline
This project implements a data pipeline to process and analyze the MovieLens 20M dataset using AWS Glue and PySpark. The pipeline involves two main stages:

Data Ingestion: Load and clean data from the MovieLens dataset (CSV) and store it in MySQL and S3.
Data Aggregation: Perform aggregations and analysis such as calculating the average movie rating, finding the most active users, and identifying the most highly-rated movie.
## 1. Setup Instructions
Prerequisites
Before running the pipeline, you need to set up a few things:

AWS Account: Make sure you have an AWS account and the necessary permissions to work with AWS Glue, S3, and MySQL.
AWS CLI: Install and configure the AWS CLI to interact with your AWS account. Install AWS CLI
Python 3.x: Ensure that you have Python 3.x installed. Install Python
Apache Spark: The project uses Spark for distributed data processing.
Kaggle Account: You'll need a Kaggle account to download the dataset.
### Dependencies
Install the required Python dependencies:

bash
Copy code
pip install boto3
pip install pyspark
pip install mysql-connector-python
Dataset
Go to the Kaggle MovieLens 20M dataset page: MovieLens 20M Dataset.

Download the dataset by either:

Using Kaggle CLI (recommended):
bash
Copy code
kaggle datasets download -d grouplens/movielens-20m-dataset
Or downloading the dataset manually from the Kaggle website and extracting the files.
The dataset contains the following files:

movies.csv: Contains movie metadata (e.g., title, genres).
ratings.csv: Contains user ratings for movies.
After downloading and extracting the dataset, upload the CSV files to your AWS S3 bucket (or use them locally for testing).

Set Up AWS Glue and MySQL
Create a MySQL Database and Table:

Create a MySQL database (e.g., movie_lens_db).
Create a table (e.g., movies and ratings) to store the movie data and ratings.
Configure your MySQL server to allow connections (you may need to configure security groups in AWS if you're using RDS).
Example schema for ratings table:

sql
Copy code
CREATE TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp INT
);
Create an S3 Bucket:

Create an S3 bucket where the data will be ingested and stored.
Example: your-s3-bucket
Create AWS Glue Role:

Create an AWS IAM role with the necessary permissions to interact with Glue, S3, and MySQL. Attach the policy for AWSGlueServiceRole.
2. Running the Pipeline
Step 1: Ingestion Job
Place the movie_data.csv (ratings and movies CSVs) in an S3 bucket or a local path that the Glue job can access.
Use the DataIngestion class to read the CSV data from S3 or MySQL and load it into MySQL.
To run the ingestion job:

python
Copy code
from ingestion import DataIngestion
from config import MYSQL_CONFIG, AWS_CONFIG

# Initialize Spark session and DataIngestion class
spark = SparkSession.builder.appName("MovieLensIngestion").getOrCreate()
data_ingestion = DataIngestion(spark, MYSQL_CONFIG)

# Read CSV data from S3 and process it
csv_path = "s3://your-bucket-name/movie_data.csv"
df = data_ingestion.read_csv(csv_path)

# Perform the ingestion process (e.g., clean, write to MySQL)
data_ingestion.write_to_mysql(df, table_name="movies")
Step 2: Aggregation Job
After ingesting the data, run the aggregation job to perform insights extraction and analysis.

To run the aggregation job:

python
Copy code
from aggregation import aggregate_data

# Run the aggregation job
aggregate_data()
The aggregation job calculates:

The average rating for each movie.
The most active users.
The most highly-rated movie.
It then stores the results in S3 in Parquet format for further analysis.

Step 3: Deploy to AWS Glue
Once you have the script ready, use the GlueJobInfra class to upload the script to S3, create the Glue job, and start the job.

To run the Glue job:

python
Copy code
from infra import GlueJobInfra
from config import AWS_CONFIG

def run_aggregation_job():
    glue_infra = GlueJobInfra(
        glue_role_arn='arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole',
        region=AWS_CONFIG['region'],
        s3_bucket=AWS_CONFIG['s3_bucket'],
        glue_script_path='scripts/movie_lens_aggregation.py',  # Path to the aggregation script in S3
        glue_job_name='MovieLensAggregationETLJob'
    )

    # Upload the Glue script to S3
    glue_infra.upload_script_to_s3()

    # Create and start Glue job
    glue_job_name = glue_infra.create_glue_job()
    job_run_id = glue_infra.start_glue_job()

    # Check the status of the Glue job
    status = glue_infra.check_glue_job_status(job_run_id)
    print(f"Glue job {status}.")

if __name__ == "__main__":
    run_aggregation_job()
3. Running Locally (Optional)
If you want to run the pipeline locally before deploying to AWS Glue, follow these steps:

Set up a local MySQL instance or use AWS RDS.
Set up S3 access (either by using local files or connecting to AWS S3).
Modify the config.py to reflect your local or cloud MySQL and S3 configuration.
Run the script as you would normally in any Python environment.
4. Expected Output
Ingestion: The processed data will be saved to MySQL (ratings and movies) and S3 in Parquet format for further use.
Aggregation: The aggregation results will be saved to S3 in Parquet format, including:
Average movie ratings.
The most active users.
The most highly-rated movie.
These outputs can be used for further analysis or reporting.

5. Cleanup
To clean up resources:

Delete the data from S3.
Drop the tables from MySQL.
Delete the Glue job (if deployed) from AWS Glue Console.
6. License
This project is licensed under the MIT License.

# Remeinder glue connection for mysql has been created, this was assumed that was created, IAM Role was assumed that exists as well