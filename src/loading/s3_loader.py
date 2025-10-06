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
    

    def write_parquet(self, df: DataFrame, file_path: str, mode: str = "append", partition_by: List[str] = None) -> None:
        """ 
        Write parquet file to S3

        Args:
            df (DataFrame): Dataframe with data to write
            file_path (str): Path to write parquet file 
            mode (str): (append/overwrite/error)
            partition_by (List[str]): Column(s) to partition the parquet 
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            logger.info(f"Writing dataframe to parquet: {full_path}")

            df.write.mode(mode).partitionBy(partition_by).parquet(full_path)
        except Exception as e:
            logger.error(f"Error writing to parquet: {full_path}: {str(e)}")
            raise


        def write_delta(self, df: DataFrame, file_path: str, mode: str = "append", partition_by: List[str] = None) -> None:
            """ 
            Write delta files to S3

            Args:
                df (DataFrame): dataframe to write
                file_path (str): Path to write delta files
                mode (str): (append/overwrite/error)
                partition_by (List[str]): Column(s) to partition the delta table
            """
            try:
                full_path = f"s3a://{self.bucket_name}/{file_path}"
                logger.info(f"Writing dataframe to delta format in path: {full_path}")

                df.write.format("delta").mode(mode).partitionBy(partition_by).save(full_path)
            except Exception as e:
                logger.error(f"Error writing delta table to path: {full_path}: {str(e)}")
                raise
            

    

        


