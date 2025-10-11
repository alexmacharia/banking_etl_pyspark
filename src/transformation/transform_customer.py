from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import logging

logger = logging.getLogger(__name__)


class CustomerTransformer:
    """ Class to handle customer transformations"""

    def __init__(self, spark: SparkSession):
        """
        Initialize the customer transformation class
        
        Args:
            spark (SparkSession): Spark session

        """
        self.spark = spark

    
    def clean_customer_data(self, df: DataFrame) -> DataFrame:
        """ 
        Clean customers data by handling missing values and data types

        Args:
            df (DataFrame): Raw customers data
    
        Returns:
            DataFrame: Cleaned customers data
    
        """
        # Convert date strings to date format
        df = df.withColumn("date_of_birth", F.to_date("date_of_birth"))

        df = df.withColumn("customer_since", F.to_date("customer_since"))

        # Convert string to int
        df = df.withColumn("credit_score", F.col("credit_score").cast("int"))

        # Handle missing values 
        df = df.fillna("Unknown", ["city", "state", "country", "zip_code", "risk_segment"])

        return df
