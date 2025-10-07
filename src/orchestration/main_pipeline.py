from pyspark.sql import SparkSession
import logging
import json
import os
from datetime import datetime

# Import project modules
from src.utils.spark_session import create_spark_session
from src.ingestion.local_connector import LocalConnector
from src.transformation.transform_transaction import TransactionTransformer
from src.transformation.data_quality import DataQualityChecker
from src.loading.redshift_loader import RedshiftLoader
from src.loading.s3_loader import S3Loader
from src.utils.logging_utils import setup_logging

logger = logging.getLogger(__name__)

class BankingETLPipeline:
    """ Main class to orchestrate the banking ETL pipeline"""

    def __init__(self, config_path: str):
        """ 
        Initialize the ETL pipeline class

        Args:
            config_path (str): Path to the config file
        """

        # Setup logging
        setup_logging()

        # Load config file
        try:
            logger.info(f"Loading configuration from {config_path}")
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading config file from {config_path}: {str(e)}")

        # Initialize spark session
        logger.info("Initializing Spark session")
        self.spark = create_spark_session(app_name=self.config.get("app_name", "Banking ETL Pipeline"))

        # Set execution date
        self.execution_date = datetime.now().strftime("%Y-%m-%d")

        # Initialize components
        self._init_components()

    
    def _init_components(self):
        """Initialized the pipeline components based on the configurations"""
        logger.info("Initializing pipeline components")


        # Initialize data connectors
        s3_config = self.config.get("s3", {})

        local_config = self.config.get("local", {})
        self.local_connector = LocalConnector(
            self.spark,
            local_config.get("base_data_path", "")
        )

        # Initialize transformers
        self.transaction_transformer = TransactionTransformer(self.spark)

        # Initialize data quality checker
        self.data_quality_cheker = DataQualityChecker(self.spark)

        self.s3_loader = S3Loader(
            self.spark,
            s3_config.get("bucket_name", "banking-data-lake-03")
            )
        

    def run_transaction_pipeline(self):
        """ Run the transaction data pipeline"""
        logger.info("Running transaction data pipeline")

        try:
            transaction_config = self.config.get("pipelines", {}).get("transactions", {})
            source_type = transaction_config.get("source_type")
            source_format = transaction_config.get("source_format")

            if source_type == "s3":
                if source_format == "csv":
                    raw_transactions = self.s3_connector.read_csv(
                        transaction_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_transactions = self.s3_connector.read_parquet(
                        transaction_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_transactions = self.s3_connector.read_json(
                        transaction_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_transactions = self.s3_connector.read_delta(
                        transaction_config.get("source_path")
                    )
                else:
                    logger.error(f"Unsupported source format {source_format}")
                    raise ValueError(f"Unsupported source format {source_format}")
            elif source_type == "rds":
                raw_transactions = self.rds_connector.read_table(
                    transaction_config.get("source_path")
                )
            elif source_type == "local":
                if source_format == "csv":
                    raw_transactions = self.s3_connector.read_csv(
                        transaction_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_transactions = self.s3_connector.read_parquet(
                        transaction_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_transactions = self.s3_connector.read_json(
                        transaction_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_transactions = self.s3_connector.read_delta(
                        transaction_config.get("source_path")
                    )
                else:
                    logger.error(f"Unsupported source format {source_format}")
                    raise ValueError(f"Unsupported source format {source_format}")
            else:
                logger.error(f"Unsupported source type {source_type}")
                raise ValueError(f"Unsupported source type {source_type}")
            
            # Transform transaction data
            cleaned_transactions = self.transaction_transformer.clean_transaction_data(raw_transactions)
            enriched_transactions = self.transaction_transformer.enrich_transaction_data(cleaned_transactions)
            transactions_with_metrics = self.transaction_transformer.calculate_transaction_metrics(enriched_transactions)
            final_transactions = self.transaction_transformer.detect_anomalies(transactions_with_metrics)

            # Run data quality checks
            quality_results = self.data_quality_checker.run_all_checks(
                final_transactions,
                transaction_config.get("data_quality", {})
            )




            

        


    




