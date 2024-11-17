import boto3
import time
from botocore.exceptions import ClientError
import os

class GlueJobInfra:
    def __init__(self, glue_role_arn, region, s3_bucket, glue_script_path, glue_job_name):
        self.glue = boto3.client('glue', region_name=region)
        self.s3 = boto3.client('s3')
        self.glue_role_arn = glue_role_arn
        self.s3_bucket = s3_bucket
        self.glue_script_path = glue_script_path
        self.glue_job_name = glue_job_name

    def upload_script_to_s3(self):
        """
        Uploads the PySpark script to the specified S3 bucket for Glue job execution.
        """
        try:
            with open(self.glue_script_path, 'rb') as script_file:
                self.s3.upload_fileobj(script_file, self.s3_bucket, self.glue_script_path)
            print(f"Script successfully uploaded to S3: s3://{self.s3_bucket}/{self.glue_script_path}")
        except Exception as e:
            print(f"Failed to upload the script: {str(e)}")
            raise

    def create_glue_job(self):
        """
        Creates an AWS Glue job.
        """
        try:
            response = self.glue.create_job(
                Name=self.glue_job_name,
                Role=self.glue_role_arn,
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': f"s3://{self.s3_bucket}/{self.glue_script_path}",
                    'PythonVersion': '3',
                },
                MaxCapacity=10.0,  # Modify based on the job size
                Timeout=2880,  # Job timeout in seconds (2 days)
                GlueVersion='2.0',  # AWS Glue version
                NumberOfWorkers=10,  # Number of workers (can be modified based on job size)
                WorkerType='Standard'  # Modify based on job resource requirement
            )
            print(f"Glue job created successfully: {response['Name']}")
            return response['Name']
        except ClientError as e:
            print(f"Failed to create Glue job: {e}")
            raise

    def start_glue_job(self):
        """
        Starts the Glue job.
        """
        try:
            job_run_response = self.glue.start_job_run(JobName=self.glue_job_name)
            print(f"Started Glue job run: {job_run_response['JobRunId']}")
            return job_run_response['JobRunId']
        except ClientError as e:
            print(f"Failed to start Glue job: {e}")
            raise

    def check_glue_job_status(self, job_run_id):
        """
        Checks the status of the Glue job run.
        """
        try:
            while True:
                job_run_status = self.glue.get_job_run(JobName=self.glue_job_name, RunId=job_run_id)
                status = job_run_status['JobRun']['JobRunState']
                print(f"Glue job status: {status}")
                if status in ['SUCCEEDED', 'FAILED', 'TIMEOUT']:
                    return status
                time.sleep(30)  # Wait for 30 seconds before checking again
        except ClientError as e:
            print(f"Failed to get Glue job status: {e}")
            raise

