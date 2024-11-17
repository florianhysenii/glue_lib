from pyspark.sql import SparkSession
import pymysql
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

class DataIngestion:
    def __init__(self, spark: SparkSession, mysql_config):
        self.spark = spark
        self.mysql_config = mysql_config

    def read_csv(self, file_path, schema=None):
        """
        Reads a CSV file into a Spark DataFrame.
        :param file_path: Path to the CSV file.
        :param schema: Schema for the CSV file.
        """
        df = self.spark.read.csv(file_path, schema=schema, header=True)
        return df

    def write_to_mysql(self, df, table_name):
        """
        Writes a Spark DataFrame to a MySQL table.
        :param df: Spark DataFrame to write.
        :param table_name: MySQL table name.
        """
        jdbc_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        properties = {
            "user": self.mysql_config["user"],
            "password": self.mysql_config["password"],
            "driver": "com.mysql.cj.jdbc.Driver",
        }
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
        print(f"Data successfully written to MySQL table: {table_name}")
