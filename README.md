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
│   └── config.json                # Configuration for data paths and parameters
├── src/
│   ├── ingestion/                 # Logic for reading raw data
│   │   ├── local_connector.py
|   │   ├── s3_connector.py       # Data cleaning and transformation
│   │   └── rds_connector.py
│   ├── transformation/                    # Writes processed data back to S3
│   │   ├── transform_account.py
│   │   ├── transform_customer.py
│   │   ├── transform_transaction.py
│   │   └── data_quality.py
│   ├── loading/                    # Writes processed data back to S3 
│   │   ├── local_loader.py
│   │   ├── redshift_loader.py
│   │   └── s3_loader.py     
│   ├── utils/                   # Helper functions (logging, I/O)
│   │   ├── spark_session.py
│   │   ├── logging_utils.py
│   │   └── data_generator.py
├── notebooks/
│   ├── accounts.ipynb          # Jupyter notebooks for exploration
│   └── customers.ipynb
├── tests/
│   ├── test_transform_account.py
│   ├── test_transform_customer.py
│   └── test_transform_transactions.py    # Unit tests for PySpark logic
├── docs/
│   ├── architecture.png
│   └── erd.png
├── requirements.txt               # Python dependencies
├── README.md                      # Project documentation
└── main.py                        # Entry point for the pipeline
```
