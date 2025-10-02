from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class RedshiftLoader:
    """ Class to handle data loading to AWS Redshift"""
    def __init__(self, spark: SparkSession, jdbc_url:str, username: str, password: str):
        """ 
        Initialize the Redshift loader class

        Args:
            spark (SparkSession): Spark session
            jdbc_url (str): Connection string for Redshift
            username (str): Redshift username
            password (str): Redshift password
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password

    
    def write_to_redshift(self, df: DataFrame, table_name: str, write_mode: str = "append",
                          preactions: Optional[str] = None, postactions: Optional[str] = None) -> None:
        """
        Write DataFrame to Redshift table

        Args:
            df (DataFrame): DataFrame to write
            table_name (str): Target table name in Redshift
            write_mode (str): Write mode (append, overwrite, error)
            preactions (Optional[str]): SQL to execute before writing
            postactions (Optional[str]): SQL to execute after writing
        """
        try:
            logger.info(f"Writing data to Redshift table: {table_name}")

            connection_properties = {
                "url": self.jdbc_url,
                "user": self.username,
                "password": self.password,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "dbtable": table_name
            }

            if preactions:
                connection_properties["preactions"] = preactions
            if postactions:
                connection_properties["postactions"] = postactions

            df.write.format("jdbc") \
                .mode(write_mode) \
                .options(**connection_properties) \
                .save()
            
            logger.info(f"Successfully wrote data to Redshift table: {table_name}")
        except Exception as e:
            logger.error(f"Error writing to Redshift table {table_name}: {str(e)}")
            raise


