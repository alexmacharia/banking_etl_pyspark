from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
import logging
from typing import List

logger = logging.getLogger(__name__)


class LocalLoader:
    """Class to handle loading to local filesystem"""


    def __init__(self, spark: SparkSession, base_path: str)
        """
        Initialize the Local Loader class
                 
        Args:
            spark (SparkSession): Sparksession
            base_path (str): base path for loading the data
        """
        self.spark = spark
        self.base_path = base_path
    
    def write_csv(self, df: DataFrame, file_path: str, header: bool = True,
                  mode: str = "overwrite") -> None:
        """
        Write csv file to Local

        Args:
            df (DataFrame): Dataframe to write
            file_path (str): Path to store csv file
            header (bool): Whether csv file will have a header
            mode (str): (overwrite/append/error)
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Writing csv file to {full_path}")

            df.write.csv(full_path, mode=mode, header=header)
        except Exception as e:
            logger.error(f"Error writing csv to {full_path}: {str(e)}")
            raise

    def write_parquet(self, df: DataFrame, file_path: str, mode: str = "append",
                      partition_by: List[str] | str = None) -> None:
        """
        Write dataframe as parquet

        Args:
            df (DataFrame): Dataframe to write
            file_path (str): Path to write parquet file
            mode (str): (overwrite/append/error)
            partition_by (List[str] | str): Partition columns
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Writing parquet file to {full_path}")

            df.write.mode(mode).partitionBy(partition_by).parquet(full_path)
        except Exception as e:
            logger.error(f"Error writing to parquet {full_path}: {str(e)}")
            raise
    
    def write_delta(self, df: DataFrame, file_path: str, mode: str = "append",
                    partition_by: List[str] | str = None) -> None:
        """
        Write dataframe to delta format

        Args:
            df (DataFrame): Dataframe to write
            file_path (str): Path to write the delta file
            mode (str): (overwrite/append/error)
            partition_by (List[str] | str): partition columns
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Writing delta file to {full_path}")

            df.write.format("delta").mode(mode).partitionBy(partition_by).save(full_path)
        except Exception as e:
            logger.error(f"Error writing delta file {full_path}: {str(e)}")
            raise

    def write_delta_upsert(self, df: DataFrame, file_path: str, key_columns: List[str] = None) -> None:
        """
        Upsert to delta table

        Args:
            df (DataFrame): Dataframe to write
            file_path (str): Path to write to
            key_columns (List[str]): Primary key columns for merging
        """
        try:
            full_path = f"{self.base_path}/{file_path}"
            logger.info(f"Upserting data to {full_path}")

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
                
            else:
                df.write.format("delta").mode("append").save(full_path)

            logger.info(f"Successfully loaded data to delta table {full_path}")
        except Exception as e:
            logger.error(f"Error upserting dataframe to delta table {full_path}: {str(e)}")
            raise