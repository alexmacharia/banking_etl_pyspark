from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name = "Banking ETL Pipeline"):
    return (
        SparkSession.builder
                    .appName(app_name).master("local[*]")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .getOrCreate()

    )