from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import StructType
from pathlib import Path


class DataIngestion:
    """
    Handles ingestion of CSV files into Spark DataFrames.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the ingestion module with a SparkSession.
        
        :param 
            spark: The active SparkSession.
        """
        self.spark = spark

    def load_csv(self, file_path: str, schema: StructType = None) -> DataFrame:
        """
        Load a CSV file into a Spark DataFrame.

        Params: 
            file_path: Path to the CSV file.
            schema: Optional Schema to enforce if available.
        
        Returns: 
            PySpark DataFrame.

        """
        try:
            # Read the CSV file into a DataFrame
            file_path = str(file_path) if isinstance(file_path, Path) else file_path

            df = self.spark.read.csv(
                file_path,
                header=True,
                inferSchema=True,
                schema=schema      # if schema available, automatically assigned
            )
            df = df.dropDuplicates()      #drop duplicate rows
            print(f"Successfully loaded file: {file_path}")
            return df
        except Exception as e:
            raise RuntimeError(f"Error loading file {file_path}: {e}")
