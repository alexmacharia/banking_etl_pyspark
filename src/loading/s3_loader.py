from pyspark.sql import SparkSession, DataFrame
import logging
from typing import List
from delta.tables import DeltaTable

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

    def write_csv(self, df: DataFrame, file_path: str, header: bool = True,
                   mode: str = "overwrite") -> None:
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
    

    def write_parquet(self, df: DataFrame, file_path: str, mode: str = "append",
                       partition_by: List[str] | str = None) -> None:
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


    def write_delta(self, df: DataFrame, file_path: str, mode: str = "append",
                     partition_by: List[str] | str = None) -> None:
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

    
    def write_delta_upsert(self, df: DataFrame, file_path: str, key_columns: List[str] = None) -> None:
        """
        Loading data to delta table through upsert

        Args:
            df (DataFrame): Dataframe with data to upsert
            flie_path (str): Path to the delta table
            key_columns (List[str]): Primary key column for merging
        """
        try:
            full_path = f"s3a://{self.bucket_name}/{file_path}"
            logger.info(f"Writing dataframe to delta location: {full_path}")

            if key_columns and len(key_columns) > 0:
                if len(key_columns) == 1:
                    key_conditions = f"target.{key_columns[0]} = source.{key_columns[0]}"
                else:
                    key_conditions = " AND ".join(f"target.{col} = source.{col}" for col in key_columns)
                non_key_columns = [col for col in df.columns if col not in key_columns]
                update_dict = {f"target.{col}": f"source.{col}" for col in non_key_columns}
                insert_dict = {f"target.{col}": f"source.{col}" for col in df.columns}

                delta_table = DeltaTable.forPath(self.spark, full_path)

                delta_table.alias("target") \
                    .merge(df.alias("source"), key_conditions) \
                    .whenMatchedUpdate(set = update_dict) \
                    .whenNotMatchedInsert(values = insert_dict) \
                    .execute()
        
            logger.info(f"Successfully loaded data to delta table {full_path}")    
        except Exception as e:
            logger.error(f"Error upserting to delta table path {full_path}: {str(e)}")
            raise

    

        


