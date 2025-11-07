from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.utils.logging_utils import ETLPipelineLogger


class S3Connector:
    """Class to ingest data from AWS S3"""
    def __init__(self, spark: SparkSession, bucket_name: str):
        """
        Initialize the class for loading data from S3

        Args:
            spark (SparkSession): Spark session
            data_path (str): Path to the data files        
        """
        self.spark = spark
        self.bucket_name = bucket_name
        self.logger = ETLPipelineLogger(__name__)

    
    def read_csv(self, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """
        Read csv from S3

        Args:
            file_path (str): Path to the csv file in the bucket
            header (bool): Does the header row exist
            infer_schema (bool): Should the schema be inferred from the data

        Returns:
            DataFrame: Spark DataFrame with loaded data        
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            self.logger.info("Reading CSV from {full_path}")

            return (
                self.spark.read
                    .option("header", header)
                    .option("inferSchema", infer_schema)
                    .csv(full_path)
            )
        except Exception as e:
            self.logger.error(f"Error reading CSV file from {full_path}" {str(e)})
            raise

    
    def read_parquet(self, file_path: str) -> DataFrame:
        """
        Read parquet file from S3

        Args:
            file_path (str): Path to parquet file

        Returns:
            DataFrame: Spark DataFrame with data
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            self.logger.info(f"Reading parquet file from {full_path}")

            return self.spark.read.parquet(full_path)
        except Exception as e:
            self.logger.error(f"Error reading parquet file from {full_path}: {str(e)}")
            raise

    
    def read_delta(self, file_path: str) -> DataFrame:
        """
        Read data from delta file in S3

        Args:
            file_path (str): Path to the delta files

        Returns:
            DataFrame: Spark DataFrame with data loaded
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            self.logger.info(f"Reading delta table from {full_path}")

            return self.spark.read.format("delta").load(full_path)
        except Exception as e:
            self.logger.error(f"Error reading delta files from {full_path}: {str(e)}")

    
   
    def read_json(self, file_path: str):
        """
        Read data from json files on S3

        Args:
            file_path (str): Path to json files on S3

        Returns:
            DataFrame: Spark DataFrame with data loaded
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            self.logger.info(f"Reading json files from {full_path}")

            self.spark.read.json(full_path)
        except Exception as e:
            self.logger.error(f"Error reading json files from S3 path {full_path}: {str(e)}")
            raise

        

    

    
    

    
