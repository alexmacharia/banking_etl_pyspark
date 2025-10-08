from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session(app_name = "Banking ETL Pipeline"):
    builder = SparkSession.builder.appName("delta_app") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0, io.delta:delta-storage_2.12:2.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    spark = configure_spark_with_delta_pip(builder) \
        .getOrCreate()

    return spark