from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.utils.logging_utils import ETLPipelineLogger

class CustomerTransformer:
    """ Class to handle customer transformations"""

    def __init__(self, spark: SparkSession):
        """
        Initialize the customer transformation class
        
        Args:
            spark (SparkSession): Spark session

        """
        self.spark = spark
        self.logger = ETLPipelineLogger(__name__)

    
    def clean_customer_data(self, df: DataFrame) -> DataFrame:
        """ 
        Clean customers data by handling missing values and data types

        Args:
            df (DataFrame): Raw customers data
    
        Returns:
            DataFrame: Cleaned customers data
    
        """
        self.logger.info("Cleaning customer data")

        # Convert date strings to date format
        df = df.withColumn("date_of_birth", F.to_date("date_of_birth"))

        df = df.withColumn("customer_since", F.to_date("customer_since"))

        # Convert string to int
        df = df.withColumn("credit_score", F.col("credit_score").cast("int"))

        # Handle missing values 
        df = df.fillna("N/A", ["city", "state", "country", "zip_code", "risk_segment"])

        return df
    

    def enrich_customer_data(self, df: DataFrame) -> DataFrame:
        """ 
        Enrich customer data

        Args:
            df (DataFrame): Raw customer data

        Returns:
            DataFrame: Enrcihed customer dataframe
        """
        self.logger.info("Enriching customer data")
    
        # Get full name
        df = df.withColumn("full_name", F.concat("first_name", F.lit(" "), "last_name"))
 
        # Combine address info to get full address
        df = df.withColumn("full_address", F.concat_ws(", ", "address", "city", "state", "zip_code", "country"))

        # Calculate the age
        df = df.withColumn("age", F.round(F.datediff(F.current_date(), "date_of_birth") / 365, 0).cast("int"))

        # Derive age group from age
        df = df.withColumn("age_group", F.when(F.col("age").between(18, 35), "18 to 35")
                                         .when(F.col("age").between(36, 50), "36 to 50")
                                         .when(F.col("age").between(51, 65), "51 to 65")
                                         .when(F.col("age") >= 66, "Over 66")
                                         .otherwise("Underage") )

        return df
