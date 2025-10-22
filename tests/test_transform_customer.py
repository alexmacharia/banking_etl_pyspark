import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as F
from src.transformation.transform_customer import CustomerTransformer
from tests.configure_test import create_spark_session as spark

customer_transformer = CustomerTransformer(spark)


@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Generate sample data for testing the customer transformation logic"""
   
    sample_data = [Row(customer_id='CUST000000', first_name='Mathew', last_name='Nabers',
                        date_of_birth='1973-09-29', email='mathew.nabers@example.com',
                          phone_number='555-782-6846', address='2759 Main St', city='Columbus',
                            state='CA', zip_code='30196', country='USA', customer_since='2015-09-19',
                              credit_score='351', risk_segment=None),
                   Row(customer_id='CUST000001', first_name='Mary', last_name='Clark',
                        date_of_birth='2004-09-21', email='mary.clark@example.com',
                          phone_number='555-315-7610', address='5117 Main St', city='Houston',
                            state='CA', zip_code=None, country='USA', customer_since='2024-09-16',
                              credit_score='654', risk_segment='Low'),
                   Row(customer_id='CUST000002', first_name='Alex', last_name='Carter',
                        date_of_birth='1960-10-02', email='alex.carter@example.com',
                          phone_number='555-815-3744', address='9649 Main St', city='Houston',
                            state=None, zip_code='97436', country='USA', customer_since='2018-09-18',
                              credit_score='607', risk_segment='Medium'),
                   Row(customer_id='CUST000003', first_name='Carol', last_name='Burrows',
                        date_of_birth='1975-09-29', email='carol.burrows@example.com',
                          phone_number='555-983-8512', address='5711 Main St', city=None,
                            state='IL', zip_code='86829', country='USA', customer_since='2017-09-18',
                              credit_score='630', risk_segment='Medium'),
                   Row(customer_id='CUST000004', first_name='Mary', last_name='Nabers',
                        date_of_birth='1967-10-01', email='mary.nabers@example.com',
                          phone_number='555-718-3541', address='2010 Main St', city='Los Angeles',
                            state='TX', zip_code='35062', country=None, customer_since='2018-09-18',
                              credit_score='456', risk_segment='Low')]
    
    cols = ['customer_id',
            'first_name',
            'last_name',
            'date_of_birth',
            'email',
            'phone_number',
            'address',
            'city',
            'state',
            'zip_code',
            'country',
            'customer_since',
            'credit_score',
            'risk_segment']
    
    return spark.createDataFrame(sample_data, cols)


def test_clean_customer_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method clean_customer_data of CustomerTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data for testing
    """
    actual_df = customer_transformer.clean_customer_data(sample_df)

    expected_df = sample_df.withColumn("date_of_birth", F.to_date("date_of_birth")) \
                           .withColumn("customer_since", F.to_date("customer_since")) \
                           .withColumn("credit_score", F.col("credit_score").cast("int")) \
                           .fillna("N/A", ["city", "state", "country", "zip_code", "risk_segment"])

    assert actual_df.collect() == expected_df.collect()


def test_enrich_customer_data(spark: SparkSession, sample_df: DataFrame) -> None:
    """
    Test method enrich_customer_data of CustomerTransformer

    Args:
        spark (SparkSession): Spark session
        sample_df (DataFrame): Sample data
    """
    actual_df = customer_transformer.enrich_customer_data(sample_df)

    expected_df = sample_df.withColumn("full_name", F.concat("first_name", F.lit(" "), "last_name")) \
                           .withColumn("full_address", F.concat_ws(", ", "address", "city", "state", "zip_code", "country")) \
                           .withColumn("age", F.round(F.datediff(F.current_date(), "date_of_birth")/ 365, 0).cast("int")) \
                           .withColumn("age_group", F.when(F.col("age").between(18, 35), "18 to 35")
                                                     .when(F.col("age").between(36, 50), "36 to 50")
                                                     .when(F.col("age").between(51, 65), "51 to 65")
                                                     .when(F.col("age") >= 66, "Over 66")
                                                     .otherwise("Underage"))
    
    assert actual_df.collect() == expected_df.collect()