from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import logging

logger = logging.getLogger(__name__)

class TransactionTransformer:
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
    

    def enrich_transaction_data(self, df: DataFrame) -> DataFrame:
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
    

    def calculate_transaction_metrics(self, df: DataFrame) -> DataFrame:
        """ 
        Calculate transaction metrics

        Args:
            df (DataFrame): Dataframe with data

        Returns:
            DataFrame: Transformed dataframe with added metrics

        """
        logger.info("Calculating trnasaction metrics")

        window =  Window.partitionBy("account_id").orderBy("transaction_date")

        df = df.withColumn("amount_signed", 
                           F.when(F.col("transaction_type").isin("deposit", "transfer"), F.col("amount_in_usd"))
                           .otherwise(-F.col("amount_in_usd")))
    
        df = df.withColumn("running_balance", F.sum("amount_signed").over(window))

        df = df.withColumn("prev_transaction_date", F.lag("transaction_date").over(window))

        df = df.withColumn("days_since_last_transaction", 
                           F.when(F.col("prev_transaction_date").isNull(), 0)
                           .otherwise(F.datediff(F.col("transaction_date"), F.col("prev_transaction_date"))))
    
        window_30d = Window.partitionBy("account_id") \
                           .orderBy(F.unix_timestamp(F.col("transaction_date"))) \
                           .rangeBetween(-2592000, 0)  # 30 days in seconds
    
        df = df.withColumn("transaction_count_30d", F.count("transaction_id").over(window_30d))
        df = df.withColumn("total_spend_30d", 
                           F.sum(F.when(F.col("transaction_type").isin("withdrawal", "payment"), F.col("amount_in_usd"))
                           .otherwise(0)).over(window_30d))
    
        return df
    

    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """ 
        Detect anomalies in transactions based on business rules

        Args:
            df (DataFrame): Transaction dataframe

        Returns:
            DataFrame: Transaction data with anomalies detected
        """
        logger.info("Detecting anomalies in transactions")

        account_stats = df.groupBy("account_id").agg(
            F.stddev("amount_in_usd").alias("amount_stddev"),
            F.avg("amount_in_usd").alias("amount_avg"),
            F.max("amount_in_usd").alias("amount_max")

        )

        df = df.join(account_stats, on="account_id", how="left")

        df = df.withColumn("is_large_transaction", 
                           (F.col("amount_in_usd") > (F.col("amount_avg") + 3 * F.col("amount_stddev"))) &
                           (F.col("amount_in_usd") > 1000))
    
        df = df.withColumn("is_unusual_location", 
                           F.col("is_international").cast("boolean") & 
                           ~F.col("location").isin("Canada", "Mexico", "United Kingdom", "France", "Germany"))
    
        df = df.withColumn("is_high_frequency",
                           F.col("transaction_count_30d") > 100)
    
        df = df.withColumn("potential_fraud", 
                           F.col("is_large_transaction") |
                           F.col("is_unusual_location") |
                           (F.col("days_since_last_transaction") < 0.01))
    
        return df
    