import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as F
from src.transformation.transform_account import AccountTransformer
from tests.configure_test import create_spark_session as spark

account_transformer = AccountTransformer(spark)

@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Generate sample data for testing the account transformation logic"""
   
    sample_data = [Row(account_id='ACC00000000', customer_id='CUST000363', account_type='investments',
                        account_status='active', open_date='2025-09-16', close_date=None, currency='GBP',
                          branch_id='BR498', interest_rate='1.2346787', balance='98345.266',
                            last_activity_date='2025-08-20'),
                   Row(account_id='ACC00000001', customer_id='CUST000209', account_type='checking',
                        account_status='closed', open_date='2022-09-17', close_date='2025-09-10',
                          currency='GBP', branch_id='BR709', interest_rate='1.2244802', balance='75040.76',
                            last_activity_date='2025-09-12'),
                   Row(account_id='ACC00000002', customer_id='CUST000796', account_type='checking',
                        account_status='active', open_date='2021-09-17', close_date=None, currency='EUR',
                          branch_id='BR262', interest_rate='1.8961781', balance='39435.562',
                            last_activity_date='2025-09-14'),
                   Row(account_id='ACC00000003', customer_id='CUST000888', account_type='investments',
                        account_status='closed', open_date='2023-09-17', close_date='2024-11-15',
                          currency='GBP', branch_id='BR999', interest_rate='0.63443124', balance='15524.992',
                            last_activity_date='2025-08-19'),
                   Row(account_id='ACC00000004', customer_id='CUST000460', account_type='investments',
                        account_status='active', open_date='2023-09-17', close_date=None, currency='GBP',
                          branch_id='BR222', interest_rate='2.9002218', balance='50014.902',
                            last_activity_date='2025-08-29')]
    
    cols = ['account_id',
            'customer_id',
            'account_type',
            'account_status',
            'open_date',
            'close_date',
            'currency',
            'branch_id',
            'interest_rate',
            'balance',
            'last_activity_date']
    
    return spark.createDataFrame(sample_data, cols)


def test_clean_account_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """ 
    Test method clean_account_data of AccountTransformer

    Args:
        spark (Spark Session): Spark Session
        sample_df (DataFrame): Sample data
    """
    actual_df = account_transformer.clean_account_data(sample_df)

    expected_df = sample_df.withColumn("open_date", F.to_date("open_date")) \
                           .withColumn("close_date", F.to_date("close_date")) \
                           .withColumn("last_activity_date", F.to_date("last_activity_date")) \
                           .withColumn("interest_rate", F.col("interest_rate").astype("decimal(3,2)")) \
                           .withColumn("balance", F.col("balance").astype("decimal(12,2)")) \
                           .fillna("N/A", ["account_type", "account_status", "currency", "branch_id"])
    
    assert actual_df.collect() == expected_df.collect()


def test_enrich_account_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method enrich_account_data of AccountTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data
    """
    actual_df = account_transformer.enrich_account_data(sample_df)

    expected_df = sample_df.withColumn("is_dormant", F.when((F.col("account_status") == "active") &
                                                            (F.datediff(F.current_date(), "last_activity_date") >= 60), True)
                                                      .otherwise(False))
    
    expected_df = expected_df.withColumn("balance_in_usd", F.when(F.col("currency") == "USD", F.col("balance"))
                                            .when(F.col("currency") == "EUR", F.col("balance") * F.lit(1.1))
                                            .when(F.col("currency") == "GBP", F.col("balance") * F.lit(1.3))
                                            .otherwise(F.col("balance")))
    
    expected_df = expected_df.withColumn("balance_in_usd", F.col("balance_in_usd").astype("decimal(12,2)"))


    assert actual_df.collect() == expected_df.collect()
    

    

    
    
    

    
