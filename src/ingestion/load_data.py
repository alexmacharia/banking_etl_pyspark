from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class LoadDataFromFile:
    """ Class to handle data ingestion from file path"""
    def __init__(self, spark: SparkSession, data_path: str):
        """
        Initialize the class for data loading.

        Args:
            spark (SparkSession): Spark session
            data_path (str): Data files path
        """
        self.spark = spark
        self.base_path = data_path

    
    def read_csv(self, file_path: str, header: bool = True, infer_schema: bool = True) -> DataFrame:
        """ 
        Read csv files from file path.

        Args:
            file_path (str): Path to the csv file in data path
            header (bool): Does the header row exist
            infer_schema (bool): Should the schema be infered from the data

        Returns:
            DataFrame: Spark DataFrame containing the csv data
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Reading csv file from {full_path}")

            return (
                self.spark.read
                    .option("header", header)
                    .option("inferSchema", infer_schema)
                    .csv(full_path)
            )
        except Exception as e:
            logger.error(f"Error reading CSV file from {full_path}: {str(e)}")
            raise
    

    def read_parquet(self, file_path: str) -> DataFrame:
        """ 
        Read parquet file from file path.

        Args:
            file_path (str): Path to the parquet files on data path

        Returns:
            DataFrame: Spark dataframe containing the data
        
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Reading parquet file from {full_path}")

            return self.spark.read.parquet(full_path)
        except Exception as e:
            logger.error(f"Error reading parquet file from {full_path}: {str(e)}")
            raise


    def read_delta(self, file_path: str) -> DataFrame:
        """ 
        Read data from delta file in file path

        Args:
            file_path (str): Path to the delta files
        
        Returns:
            DataFrame: Spark DataFrame containing data
        
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info("Reading delta file from {full_path}")

            return self.spark.read.format("delta").load(full_path)
        except Exception as e:
            logger.error(f"Error reading delta files from {full_path}: {str(e)}")


    def read_json(self, file_path: str) -> DataFrame:
        """ 
        Read json file from file path

        Args:
            file_path (str): Path to the json file

        Returns:
            DataFrame: Spark dataframe with data
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Reading json file from {full_path}")

            return self.spark.read.json(full_path)
        except Exception as e:
            logger.error(f"Error reading json files from {full_path}: {str(e)}")
            raise
