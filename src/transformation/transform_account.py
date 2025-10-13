from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import logging

logger = logging.getLogger(__name__)


class AccountTransformer:
    """Class to handle account transformations"""

    def __init__(self, spark: SparkSession):
        """
        Initialize the account transformation class

        Args:
            spark (SparkSession): Spark session
        """
        self.spark = spark

    
    def clean_account_data(self, df: DataFrame) -> DataFrame:
        """
        Clean accounts data by handling missing values and data types

        Args:
            df (DataFrame): Raw accounts data
        
        Returns:
            DataFrame: Cleaned accounts dataframe
        """
        logger.info("Cleaning account data")

        # Convert date strings to dates
        df = df.withColumn("open_date", F.to_date("open_date"))

        df = df.withColumn("close_date", F.to_date("close_date"))

        df = df.withColumn("last_activity_date", F.to_date("last_activity_date"))

        # Convert string type to numeric
        df = df.withColumn("interest_rate", F.col("interest_rate").astype("decimal(3,2)"))

        df = df.withColumn("balance", F.col("balance").astype("decimal(12,2)"))

        # Fill missing values
        df = df.fillna("N/A", ["account_type", "account_status", "currency", "branch_id"])

        return df
    

    def enrich_account_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich account data

        Args:
            df (DataFrame): Raw account data

        Returns:
            DataFrame: Enriched account dataframe
        """
        logger.info("Enriching account data")
    
        # Check if account is dormant
        df = df.withColumn("is_dormant", F.when((F.col("account_status") == "active") & (F.datediff(F.current_date(), "last_activity_date") >= 60), True)
                                      .otherwise(False))
        
        # convert balance to usd
        df = df.withColumn("balance_in_usd", F.when(F.col("currency") == "USD", F.col("balance"))
                                          .when(F.col("currency") == "EUR", F.col("balance") * F.lit(1.1))
                                          .when(F.col("currency") == "GBP", F.col("balance") * F.lit(1.3))
                                          .otherwise(F.col("balance")))
        
        # change usd balance to decimal type
        df = df.withColumn("balance_in_usd", F.col("balance_in_usd").astype("decimal(12,2)"))
    
        return df