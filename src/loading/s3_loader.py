from pyspark.sql import SparkSession, DataFrame
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

class S3Loader:
    """ Class to handle data loading to AWS S3"""

    def __init__(self, spark: SparkSession, bucket_name: str):
        """ 
        Initialize the S3 loader class

        Args:
            spark (SparkSession): Spark session
            bucket_name (str): S3 bucket name
        """
        self.spark = spark
        self.bucket_name = bucket_name

    def write_csv(self, df: DataFrame, file_path: str, header: bool = True, mode: str = "overwrite") -> None:
        """ 
        Write CSV file to S3

        Args:
            df (DataFrame): DataFrame with data to write
            file_path (str): Path to write the CSV file in S3
            header (bool): Whether the CSV will have a header
            mode (str): (overwrite/append/error)
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            logger.info("Writing Dataframe as CSV to: {full_path}")

            df.write.csv(full_path, mode=mode, header=header)
        except Exception as e:
            logger.error(f"Error writing DataFrame as CSV to {full_path}: {str(e)}")
            raise
    

    


