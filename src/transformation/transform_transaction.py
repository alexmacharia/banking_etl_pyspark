from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import logging

logger = logging.getLogger(__name__)

class TransformTransaction:
    """ Class to handle transactions transformations"""

    def __init__(self, spark: SparkSession):
        """Initialize the transactions transformation class
        
           Args:
               spark (SparkSession): Spark session
        """
        self.spark = spark

    
    def clean_transaction_data(self, df: DataFrame) -> DataFrame:
        """ 
        Clean transaction data by handling missing values and data types

        Args:
            df (DataFrame): Raw transaction data

        Returns:
            df (DataFrame): Cleaned transaction data
        """
        logger.info("Cleaning transaction data")
        
        # Convert date string to timestamp format
        df = df.withColumn("transaction_date", F.to_timestamp(F.col("transaction_date")))
        
        # Handle missing values
        df = df.na.fill("Unknown", ["merchant_name", "merchant_category", "description"])
        
        # Filter invalid transactions
        df = df.filter(~((F.col("transaction_type") == "deposit") & (F.col("amount") < 0)))
        
        # Standardize transaction types
        df = df.withColumn("transaction_type", F.lower(F.col("transaction_type")) )

        return df
    

    def enrich_transaction_data( df: DataFrame) -> DataFrame:
        """ 
        Enrich transaction data

        Args:
            df (DataFrame): Raw data

        Returns:
           DataFrame: Enriched dataframe
        """
        logger.info("Enriching transaction data")

        df = df.withColumn("year_month", 
                           F.concat(F.year(F.col("transaction_date")).cast("string"),  
                                    F.lpad(F.month(F.col("transaction_date")).cast("string"), 2, "0")) )
        
        df = df.withColumn("transaction_dow", F.dayofweek(F.col("transaction_date")))

        df = df.withColumn("is_weekend", F.when(F.col("transaction_dow").isin(1,7), True)
                           .otherwise(False))
        
        df = df.withColumn("amount_in_usd", 
                           F.when(F.col("currency") == "USD", F.col("amount"))
                           .when(F.col("currency") == "EUR", F.col("amount") * 1.1)
                           .when(F.col("currency") == "GBP", F.col("amount") * 1.3)
                           .otherwise(F.col("amount")))
        
        df = df.withColumn("transaction_category", 
                           F.when(F.col("merchant_category").isin("grocery", "supermarket", "retail"), "Retail")
                           .when(F.col("merchant_category").isin("restaurant", "fast food", "entertainment"), "Entertainment")
                           .when(F.col("merchant_category").isin("gas", "fuel", "travel"), "Transportation")
                           .when(F.col("merchant_category").isin("utility", "electricity", "water"), "Utilities")
                           .otherwise("Other"))
    
        return df
    