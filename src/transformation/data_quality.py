from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple 
import logging

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """ Class for checking data quality on the data"""

    def __init__(self, spark:SparkSession):
        """ 
        Initialize the DQ class

        Args:
            spark (SparkSession): Spark session
        """

    
    def check_nulls(self, df:DataFrame, required_cols: List[str]) -> Tuple[bool, Dict[str, int]]:
        """ 
        Check for null values in the specified fields

        Args:
            df (DataFrame): Dataframe with data to be checked
            required_cols (List[str]): List of columns to check for null values

        Returns:
            Tuple[bool, Dict[str, int]]: (passed/failed, dict of null counts by column)
        """
        logger.info(f"checking for null columns in: {required_cols}")

        null_counts = {}
        for col in required_cols:
            if col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                null_counts[col] = null_count
            else:
                logger.warning(f"Column {col} not found in the DataFrame")
                null_counts[col] = "Column not found"

        has_nulls = any(isinstance(count, int) and count > 0 for count in null_counts.values())

        if has_nulls:
             logger.warning(f"Null check failed. Null counts: {null_counts}")
             return False, null_counts
        else:
             logger.info("Null check passed")
             return True, null_counts 
        
    
    def check_duplicates(self, df:DataFrame, key_columns: List[str]) -> Tuple[bool, int]:
        """ 
        Check for duplicate records based on keys

        Args:
            df (DataFrame): Dataframe with data to be checked

            key_columns(List[str]): columns that should compose a unique key

        Returns:
            Tuple[bool, int]: (passsed/failed, count of duplicate records)
        """
        logger.info(f"Checking for duplicates on key columns")

        total_rows = df.count()

        distinct_rows = df.select(key_columns).distinct().count()

        duplicate_count = total_rows - distinct_rows

        if duplicate_count > 0:
            logger.warning(f"Duplicate check failed. Found {duplicate_count} duplicates")
            return False, duplicate_count
        else:
            logger.info("Duplicate check has passed")
            return True, 0
        
    
    def check_data_ranges(df: DataFrame, range_checks: Dict[str, Tuple]) -> Tuple[bool, Dict[str, int]]:
        """ 
        Check if values in column fall in expected range

        Args:
            df (DataFrame): Dataframe to check
            range_checks (Dict[str, Tuple]): Dictionary mapping columns to range of values

        Returns:
            Tuple[bool, Dict[str, int]]: (passed/failed, dict of out of range counts by column)
        """
        logger.info(f"Checking data ranges for columns: {list(range_checks.keys())}")

        out_of_range_counts = {}

        for col, (min_value, max_value) in range_checks.items():
            if col in df.columns:
                out_of_range_count = df.filter(
                    (F.col(col) < min_value) | (F.col(col) > max_value)
                ).count()

                out_of_range_counts[col] = out_of_range_count
            else:
                logger.warning(f"Column {col} not found")
                out_of_range_counts[col] = "Column not found"

        has_out_of_range = any(isinstance(count, int) and count > 0 for count in out_of_range_counts.values())

        if has_out_of_range:
            logger.warning(f"Range check failed. Out of range counts: {out_of_range_counts}")
            return False, out_of_range_counts
        else:
            logger.info("Range check passed")
            return True, out_of_range_counts

        