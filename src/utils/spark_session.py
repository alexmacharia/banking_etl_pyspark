from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name = "Banking ETL Pipeline"):
    return (
        SparkSession.builder
                    .appName(app_name).master("local[*]")
                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0, io.delta:delta-storage_2.12:2.2.0")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .getOrCreate()

    )