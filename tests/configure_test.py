import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def create_spark_session(app_name = "Test Spark Session"):
    """Create a local Spark session for testing"""
    spark = SparkSession.builder.appName(app_name) \
                .master("local[1]") \
                .config("spark.sql.shuffle.partitions", "1") \
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0, io.delta:delta-storage_2.12:2.2.0") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
                .getOrCreate()
    
    return spark
    
