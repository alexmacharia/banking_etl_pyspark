# Data Engineering Project with Pyspark and AWS for Banking Domain

## Overview

This project demonstrates a data engineering pipeline built using PySpark for distributed data processing and Amazon S3 for scalable data storage. This pipeline extracts raw data in various formats, transforms it using Pyspark, and loads the processed data to S3 in an optimized format (delta) or Redshift.

## Architecture

![Project Architecture](docs/architecture.png)

## Features

* **Data ingestion** from multiple sources (CSV, JSON, Parquet)
* **Data transformation** and cleaning using Pyspark
* **Data quality** checks and validations using Pyspark
* **Partitioning** and storage in optimized delta format and Redshift
* **Integration with AWS S3** for scalable storage
* **Configurable pipeline** with JSON configuration files

## Tech Stack

|  Component       |    Technology                  |
|----------------- |:------------------------------:|
|  Language        |    Python 3.9+                 |
|  Processing      |    Pyspark                     |
|  Cloud Storage   |    AWS S3                      |
|  Configuration   |    JSON                        |
|  Optional        |    AWS Glue, EMR, Databricks   |

## Project Structure

```
banking_etl_pyspark/
│
├── config/
│   └── config.json                     # Configuration parameters
├── src/
│   ├── ingestion/                      # Data ingestion modules
│   │   ├── local_connector.py          # Local filesystem connector
|   │   ├── s3_connector.py             # AWS S3 connector
│   │   └── rds_connector.py            # AWS RDS connector
│   ├── transformation/                 # Data transformation modules
│   │   ├── transform_account.py        # Account data transformations
│   │   ├── transform_customer.py       # Customer data transformations
│   │   ├── transform_transaction.py    # Transactions data transformations
│   │   └── data_quality.py             # Data quality checks
│   ├── loading/                        # Data loading modules
│   │   ├── local_loader.py             # Local filesystem loader
│   │   ├── redshift_loader.py          # AWS Redshift loader
│   │   └── s3_loader.py                # AWS S3 loader
│   ├── utils/                          # Helper functions (logging, spark session)
│   │   ├── spark_session.py            # Spark session
│   │   ├── logging_utils.py            # Logging utilities
│   │   └── data_generator.py           # Data generator utility
├── notebooks/                          # Data exploration notebooks
│   ├── accounts.ipynb                  # Accounts data exploration
│   └── customers.ipynb                 # Customer data exploration
├── tests/                              # Unit and integration tests
│   ├── test_transform_account.py       # Test account transformation module
│   ├── test_transform_customer.py      # Test customer transformation module
│   └── test_transform_transactions.py  # Test transaction transformation module
├── docs/                               # Documentation
│   ├── architecture.png                # Architecture diagram
│   └── erd.png                         # Entity relationship diagram
├── requirements.txt                    # Python dependencies
├── README.md                           # Project documentation
└── pytest.ini                          # Pytest configuration
```

## Example Configuration (config.json)
```
json

{
    "app_name": "Banking ETL Pipeline",
    "environment": "production",
    "pipelines_to_run": ["transaction", "customer", "account"],

    "s3": {
        "bucket_name": "banking-data-lake-003",
        "region": "eu-west-1"
    },

    "local": {
        "base_data_path": "data"
    },

    "pipelines": {
        "transaction": {
            "source_type": "s3",
            "source_path": "raw/transactions/",
            "source_format": "csv",
            "target_type": "s3",
            "target_path": "processed/transactions/",
            "write_mode": "append",
            "partition_cols": ["year_month"],
            "fail_on_quality_check": false,
            "data_quality": {
                "table_name": "fact_transaction",
                "required_columns": ["transaction_id", "account_id", "transaction_date", "amount"],
                "key_columns": ["transaction_id"],
                "range_checks": {
                    "amount": [500, 5000]
                }
            }
        }
    }
}
```

## Testing
To run unit tests for the Pyspark transformations
```
bash

pytest
```

## Data quality checks
These include
* Null/missing value checks
* Duplicate checks
* Data range checks
* Referential integrity checks

## Future enhancements
* Pipeline orchestration with **Databricks Jobs** or **Airflow** 
* Metadata management via **AWS Glue Catalog**
* Automated data validation with **Great Expectations**
* Integration with **Snowflake**

## Author
**Alex Macharia**
*Data Engineer*
[LinkedIn](https://www.linkedin.com/in/alex-macharia-972a9610/)