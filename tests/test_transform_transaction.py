import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from src.transformation.transform_transaction import TransactionTransformer
from tests.configure_test import create_spark_session as spark

transaction_transformer = TransactionTransformer(spark)


@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Generate sample data for testing the transaction transformation logic"""

    sample_data = [Row(transaction_id='d0d6cf2c-a995-4142-986d-d6272134eee0', account_id='ACC00000049',
                        transaction_date='2025-08-06T22:43:40.399+03:00', transaction_type='withdrawal',
                          amount='3387.0525', currency='GBP', description='Withdrawal at Chipotle',
                            merchant_name='Chipotle', merchant_category='restaurant',
                              transaction_status='pending', channel='mobile', location='USA',
                                is_international='false'),
                   Row(transaction_id='719d39a3-4eae-4479-834f-6465b26a186a', account_id='ACC00000638',
                        transaction_date='2025-06-19T19:03:40.399+03:00', transaction_type='deposit',
                          amount='1858.1637', currency='EUR', description='Deposit at Expedia',
                            merchant_name='Expedia', merchant_category='travel', transaction_status='pending',
                              channel='branch', location='Brazil', is_international='true'),
                   Row(transaction_id='9a408bc9-472d-4f17-8974-5b5791a5da85', account_id='ACC00000452',
                        transaction_date='2025-08-18T23:58:40.399+03:00', transaction_type='payment',
                          amount='2017.678', currency='GBP', description='Payment at Best Buy',
                            merchant_name='Best Buy', merchant_category='retail',
                              transaction_status='completed', channel='mobile', location='UK',
                                is_international='true'),
                   Row(transaction_id='70170917-0efe-45db-93f0-d9f340fe182c', account_id='ACC00001162',
                        transaction_date='2025-08-29T03:38:40.399+03:00', transaction_type='transfer',
                          amount='4806.1655', currency='EUR', description='Transfer at Disney+',
                            merchant_name='Disney+', merchant_category='entertainment',
                              transaction_status='reversed', channel='mobile', location='China',
                                is_international='true'),
                   Row(transaction_id='d880c9fb-5eed-4028-9e63-1ed6cbdf0de3', account_id='ACC00001461',
                        transaction_date='2025-08-24T06:10:40.399+03:00', transaction_type='withdrawal',
                          amount='119.79462', currency='EUR', description='Withdrawal at Delta Airlines',
                            merchant_name='Delta Airlines', merchant_category='travel',
                              transaction_status='failed', channel='branch', location='China',
                                is_international='true')]
    
    cols = ['transaction_id',
            'account_id',
            'transaction_date',
            'transaction_type',
            'amount',
            'currency',
            'description',
            'merchant_name',
            'merchant_category',
            'transaction_status',
            'channel',
            'location',
            'is_international']
    
    return spark.createDataFrame(sample_data, cols)


def test_clean_transaction_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method clean_transaction_data of TransactionTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data for testing
    """
    actual_df = transaction_transformer.clean_transaction_data(sample_df)

    expected_df = sample_df.withColumn("transaction_date", F.to_timestamp(F.col("transaction_date"))) \
                           .na.fill("N/A", ["merchant_name", "merchant_category", "description"]) \
                           .filter(~((F.col("transaction_type") == "deposit") & (F.col("amount") < 0))) \
                           .withColumn("transaction_type", F.lower(F.col("transaction_type")))
    
    assert actual_df.collect() == expected_df.collect()


def test_enrich_transaction_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method enrich_transaction_data of TransactionTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): sample data for testing
    
    """
    actual_df = transaction_transformer.enrich_transaction_data(sample_df)

    expected_df = sample_df.withColumn("year_month", F.date_format("transaction_date", "yyyyMM")) \
                           .withColumn("transaction_dow", F.dayofweek(F.col("transaction_date"))) \
                           .withColumn("is_weekend", F.when(F.col("transaction_dow").isin(1,7), True)
                                       .otherwise(False)) \
                           .withColumn("amount_in_usd", 
                                       F.when(F.col("currency") == "USD", F.col("amount"))
                                        .when(F.col("currency") == "EUR", F.col("amount") * 1.1)
                                        .when(F.col("currency") == "GBP", F.col("amount") * 1.3)
                                        .otherwise(F.col("amount"))) \
                            .withColumn("transaction_category", 
                                       F.when(F.col("merchant_category").isin("grocery", "supermarket", "retail"), "Retail")
                                        .when(F.col("merchant_category").isin("restaurant", "fast food", "entertainment"), "Entertainment")
                                        .when(F.col("merchant_category").isin("gas", "fuel", "travel"), "Transportation")
                                        .when(F.col("merchant_category").isin("utility", "electricity", "water"), "Utilities")
                                        .otherwise("Other"))
    
    assert actual_df.collect() == expected_df.collect()


def test_calculate_transaction_metrics(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method calculate_transaction_metrics of transactionTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data for testing
    
    """
    sample_data = transaction_transformer.enrich_transaction_data(
        transaction_transformer.clean_transaction_data(sample_df)
    )

    actual_df = transaction_transformer.calculate_transaction_metrics(sample_data)

    window = Window.partitionBy("account_id").orderBy("transaction_date")

    expected_df = sample_data.withColumn("amount_signed", 
                                       F.when(F.col("transaction_type").isin("deposit", "transfer"), F.col("amount_in_usd"))
                                       .otherwise(-F.col("amount_in_usd")))
    
    expected_df = expected_df.withColumn("running_balance", F.sum("amount_signed").over(window)) \
                             .withColumn("prev_transaction_date", F.lag("transaction_date").over(window))
    
    expected_df = expected_df.withColumn("days_since_last_transaction", 
                                         F.when(F.col("prev_transaction_date").isNull(), 0)
                                         .otherwise(F.datediff(F.col("transaction_date"), F.col("prev_transaction_date"))))
    
    window_30d = Window.partitionBy("account_id") \
                       .orderBy(F.unix_timestamp(F.col("transaction_date"))) \
                       .rangeBetween(-2592000, 0)
    
    expected_df = expected_df.withColumn("transaction_count_30d", F.count("transaction_id").over(window_30d)) \
                             .withColumn("total_spend_30d", 
                                         F.sum(F.when(F.col("transaction_type").isin("withdrawal", "payment"), F.col("amount_in_usd"))
                                               .otherwise(0)).over(window_30d))
    
    assert actual_df.collect() == expected_df.collect()


def test_detect_anomalies(spark: SparkSession, sample_df: DataFrame) -> None:
    """ 
    Test method detect_anomalies of TransactionTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data to test
    """
    sample_data = transaction_transformer.calculate_transaction_metrics(
        transaction_transformer.enrich_transaction_data(
            transaction_transformer.clean_transaction_data(sample_df)
        )
    )

    actual_df = transaction_transformer.detect_anomalies(sample_data)

    account_stats = sample_data.groupBy("account_id").agg(
        F.stddev("amount_in_usd").alias("amount_stddev"),
        F.avg("amount_in_usd").alias("amount_avg"),
        F.max("amount_in_usd").alias("amount_max")
    )

    expected_df = sample_data.join(account_stats, on="account_id", how="left")

    expected_df = expected_df.withColumn("is_large_transaction", 
                                         (F.col("amount_in_usd") > (F.col("amount_avg") + 3 * F.col("amount_stddev"))) &
                                         (F.col("amount_in_usd") > 1000)
                                         )
    
    expected_df = expected_df.withColumn("is_unusual_location",
                                         F.col("is_international").cast("boolean") &
                                         ~F.col("location").isin("Canada", "Mexico", "United Kingdom", "France", "Germany"))
    
    expected_df = expected_df.withColumn("is_high_frequency",
                                         F.col("transaction_count_30d") > 100)
    
    expected_df = expected_df.withColumn("potential_fraud", 
                                         F.col("is_large_transaction") |
                                         F.col("is_unusual_location") |
                                         F.col("is_high_frequency") |
                                         (F.col("days_since_last_transaction") < 0.01))
    
    assert actual_df.collect() == expected_df.collect()


    


