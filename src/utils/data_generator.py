from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, StringType, TimestampType, StructField, BooleanType,
    StructType, ArrayType, DecimalType, DateType
)
import random
from datetime import datetime, timedelta
import uuid
#import dbldatagen as dg


spark = SparkSession.builder.master("local[*]").appName("Generate Sample Data").getOrCreate()


customer_schema = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("date_of_birth", DateType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("customer_since", DateType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("risk_segment", StringType(), True)
    ]
)

account_schema = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("account_status", StringType(), True),
    StructField("open_date", DateType(), True),
    StructField("close_date", DateType(), True),
    StructField("currency", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("interest_rate", FloatType(), True),
    StructField("balance", FloatType(), True),
    StructField("last_activity_date", DateType(), True)

])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("description", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("location", StringType(), True),
    StructField("is_international", BooleanType(), True)
])


def generate_customer_data(num_customers=1000):
    first_names = ["Alex", "Fred", "John", "Mary", "Carol", "Anne", "Mathew", "Simon", "Faith"]
    last_names = ["Clark", "Kelly", "Grisham", "Carter", "Nabers", "Montana", "Burrows", "Jackson"]
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    cities = ["Los Angeles", "New York", "Houston", "Miami", "Chicago", "Philadelphia", "Columbus", "Atlanta", "Charlotte", "Detroit"]
    risk_segments = ["Low", "Medium", "High"]

    customers = []

    for i in range(num_customers):
        customer_id = f"CUST{i:06d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        years_ago = random.randint(20,80)
        dob = datetime.now() - timedelta(days=365 * years_ago)
        
        years_customer = random.randint(0,10)
        customer_since = datetime.now() - timedelta(days=365 * years_customer)

        state = random.choice(states)
        city = random.choice(cities)

        customers.append((customer_id,
                          first_name,
                          last_name,
                          dob.date(),
                          f"{first_name.lower()}.{last_name.lower()}@example.com",
                          f"555-{random.randint(100, 999)}-{random.randint(1000,9999)}",
                          f"{random.randint(100,9999)} Main St",
                          city,
                          state,
                          f"{random.randint(10000,99999)}",
                          "USA",
                          customer_since.date(),
                          random.randint(300, 850),
                          random.choice(risk_segments)
                          ))
        
        
    return spark.createDataFrame(customers, customer_schema)


def generate_accounts_data(customers_df, num_accounts = 1500):
    account_types = ["checking", "savings", "investments"]
    account_statuses = ["active", "closed", "suspended"]
    currencies = ["USD", "EUR", "GBP"]

    accounts = []

    customer_ids = [row.customer_id for row in customers_df.select("customer_id").collect()]

    for i in range(num_accounts):
        account_id = f"ACC{i:08d}"
        customer_id = random.choice(customer_ids)
        account_type = random.choice(account_types)
        account_status = random.choice(account_statuses)

        years_ago = random.randint(0,5)
        open_date = datetime.now() - timedelta(days=365 * years_ago)

        if account_status == "closed":
            days_ago = random.randint(0, 365)
            close_date = datetime.now() - timedelta(days=days_ago)
        else:
            close_date = None

        
        days_ago = random.randint(0,30)
        last_activity_date = datetime.now() - timedelta(days=days_ago)

        accounts.append((
            account_id,
            customer_id,
            account_type,
            account_status,
            open_date.date(),
            close_date,
            random.choice(currencies),
            f"BR{random.randint(100,999)}",
            random.uniform(0.01, 5.0),
            random.uniform(0, 100000),
            last_activity_date.date()
        ))
        print(accounts[-1])

    return spark.createDataFrame(accounts, account_schema)


def generate_transaction_data(accounts_df, num_transactions=10000):
    transaction_types = ['deposit', 'withdrawal', 'transfer', 'payment']
    currencies = ["USD", "EUR", "GBP"]
    
        

customer_df = generate_customer_data(1000)  
customer_df.write.mode("overwrite").csv("data/raw/customers/", header=True)

account_df = generate_accounts_data(customer_df, 1500)
account_df.write.mode("overwrite").csv("data/raw/accounts/", header=True)

customer_df.show(10)

temp = """ 
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

"""

spark.stop()


