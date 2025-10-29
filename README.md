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
