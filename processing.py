from pyspark.sql.functions import col, current_timestamp

class DataProcessor:
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def clean_data(self):
        """
        Cleans the data by removing duplicates and null values.
        """
        self.dataframe = self.dataframe.dropDuplicates().dropna()
        return self.dataframe

    def transform_data(self):
        """
        Adds a processed timestamp column.
        """
        self.dataframe = self.dataframe.withColumn("processed_at", current_timestamp())
        return self.dataframe
