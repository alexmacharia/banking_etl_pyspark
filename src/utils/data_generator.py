from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, StringType, TimestampType, StructField, BooleanType,
    StructType, ArrayType, DecimalType, DateType
)
import random
from datetime import datetime
import uuid
import dbldatagen as dg


spark = SparkSession.builder.master("local[*]").appName("Generate Sample Data").getOrCreate()

row_count = 10_000

data_spec = (
    dg.DataGenerator(name="customers", rows=row_count)
    .withIdOutput()
    .withColumn("customer_id", IntegerType(), minValue=1, maxValue=10000)
    .withColumn("first_name", StringType(), values=["Alex", "Fred", "John", "Mary", "Carol", "Anne", "Mathew", "Simon", "Faith"], random=True)
    .withColumn("last_name", StringType(), values=["Clark", "Kelly", "Grisham", "Carter", "Nabers", "Montana", "Burrows", "Jackson"], random=True)
    #.withColumn("date_of_birth", DateType(), begin="1959-01-01", end="2005-12-31", random=True)
    #.withColumn("customer_since", )
    .withColumn("city", StringType(), values=["Chicago, IL", "Philadelphia, PA", "Columbus, OH", "Atlanta, GA", "Houston, TX", "Miami, FL", "Los Angeles, CA", "New York, NY",  "Charlotte, NC", "Detroit, MC"], random=True)
    .withColumn("risk_segments", StringType(), values=["Low", "Medium", "High"], random=True)
    .withColumn("email", StringType(), template=r'\w.\w@\w.com|\w@\w.co.u\k')



)

df = data_spec.build()

df.show()



spark.stop()


