import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_spark_session(app_name = "Test Spark Session"):
    """Create a local Spark session for testing"""
    spark = SparkSession.builder.appName(app_name) \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()
    
    return spark
    
