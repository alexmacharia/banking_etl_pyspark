import random
from datetime import datetime
import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType
from src.transformation.transform_account import AccountTransformer
from tests.configure_test import create_spark_session


spark = create_spark_session()

account_transformer = AccountTransformer(spark)

@pytest.fixture
def sample_df(spark: SparkSession) -> DataFrame:
    """Generate sample data for testing the transformation logic"""
   
    sample_data = [Row(customer_id='CUST000000', first_name='John', last_name='Burrows',
                        date_of_birth=datetime.date(1982, 11, 1), email='john.burrows@example.com',
                          phone_number='555-220-9392', address='6789 Main St', city='Columbus',
                            state='GA', zip_code='26178', country='USA', customer_since=datetime.date(2021, 10, 22),
                              credit_score=311, risk_segment='High'),
                   Row(customer_id='CUST000001', first_name='Anne', last_name='Jackson',
                        date_of_birth=datetime.date(1964, 11, 5), email='anne.jackson@example.com',
                          phone_number='555-327-1464', address='1664 Main St', city='Houston',
                            state='OH', zip_code='86387', country='USA', customer_since=datetime.date(2024, 10, 21),
                              credit_score=473, risk_segment='Low'),
                   Row(customer_id='CUST000002', first_name='Anne', last_name='Carter',
                        date_of_birth=datetime.date(1951, 11, 9), email='anne.carter@example.com',
                          phone_number='555-198-9445', address='957 Main St', city='Atlanta',
                            state='GA', zip_code='80882', country='USA', customer_since=datetime.date(2016, 10, 23),
                              credit_score=776, risk_segment='Medium'),
                   Row(customer_id='CUST000003', first_name='Simon', last_name='Grisham',
                        date_of_birth=datetime.date(1977, 11, 2), email='simon.grisham@example.com',
                          phone_number='555-511-8416', address='8970 Main St', city='Philadelphia',
                            state='PA', zip_code='39372', country='USA', customer_since=datetime.date(2017, 10, 23),
                              credit_score=792, risk_segment='Low'),
                   Row(customer_id='CUST000004', first_name='Anne', last_name='Kelly',
                        date_of_birth=datetime.date(1948, 11, 9), email='anne.kelly@example.com',
                          phone_number='555-298-9963', address='2641 Main St', city='Chicago',
                            state='OH', zip_code='13516', country='USA', customer_since=datetime.date(2018, 10, 23),
                              credit_score=801, risk_segment='Low')]
    
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


def test_clean_account_data(spark: SparkSession, sample_data: DataFrame) -> None:
    actual_result = account_transformer.clean_account_data(sample_data)
    

    
