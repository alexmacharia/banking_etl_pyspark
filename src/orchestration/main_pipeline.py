from pyspark.sql import SparkSession
import logging
import json
import os
from datetime import datetime

# Import project modules
from src.utils.spark_session import create_spark_session
from src.ingestion.local_connector import LocalConnector
from src.transformation.transform_customer import CustomerTransformer
from src.transformation.transform_account import AccountTransformer
from src.transformation.transform_transaction import TransactionTransformer
from src.transformation.data_quality import DataQualityChecker
from src.loading.redshift_loader import RedshiftLoader
from src.loading.s3_loader import S3Loader
from src.loading.local_loader import LocalLoader
from src.utils.logging_utils import ETLPipelineLogger

class BankingETLPipeline:
    """ Main class to orchestrate the banking ETL pipeline"""

    def __init__(self, config_path: str):
        """ 
        Initialize the ETL pipeline class

        Args:
            config_path (str): Path to the config file
        """
        self.logger = ETLPipelineLogger(__name__)

        # Load config file
        try:
            self.logger.info(f"Loading configuration from {config_path}")
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading config file from {config_path}: {str(e)}")

        # Initialize spark session
        self.logger.info("Initializing Spark session")
        self.spark = create_spark_session(app_name=self.config.get("app_name", "Banking ETL Pipeline"))

        # Set execution date
        self.execution_date = datetime.now().strftime("%Y-%m-%d")

        # Initialize components
        self._init_components()

    
    def _init_components(self):
        """Initialized the pipeline components based on the configurations"""
        self.logger.info("Initializing pipeline components")


        # Initialize data connectors
        s3_config = self.config.get("s3", {})

        local_config = self.config.get("local", {})
        self.local_connector = LocalConnector(
            self.spark,
            local_config.get("base_data_path", "")
        )

        # Initialize transformers
        self.transaction_transformer = TransactionTransformer(self.spark)
        self.customer_transformer = CustomerTransformer(self.spark)
        self.account_transformer = AccountTransformer(self.spark)

        # Initialize data quality checker
        self.data_quality_checker = DataQualityChecker(self.spark)
         
        # Initialize data loaders
        redshift_config = self.config.get("redshift", {})
        self.redshift_loader = RedshiftLoader(
            self.spark,
            redshift_config.get("jdbc_url"),
            redshift_config.get("username"),
            redshift_config.get("password")
        )

        self.s3_loader = S3Loader(
            self.spark,
            s3_config.get("bucket_name", "banking-data-lake-03")
            )
        
        self.local_loader = LocalLoader(
            self.spark,
            local_config.get("base_data_path", "")
        )
        

    def run_transaction_pipeline(self):
        """ Run the transaction data pipeline"""
        self.logger.info("Running transaction data pipeline")

        try:
            transaction_config = self.config.get("pipelines", {}).get("transaction", {})
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
                    raise ValueError(f"Unsupported source format {source_format}")
            elif source_type == "rds":
                raw_transactions = self.rds_connector.read_table(
                    transaction_config.get("source_path")
                )
            elif source_type == "local":
                if source_format == "csv":
                    raw_transactions = self.local_connector.read_csv(
                        transaction_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_transactions = self.local_connector.read_parquet(
                        transaction_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_transactions = self.local_connector.read_json(
                        transaction_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_transactions = self.local_connector.read_delta(
                        transaction_config.get("source_path")
                    )
                else:
                    raise ValueError(f"Unsupported source format {source_format}")
            else:
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
            
            if not quality_results.get("overall_passed", False):
                self.logger.warning("Data quality checks failed for transaction data")
                if transaction_config.get("fail_on_quality_check", True):
                    raise Exception("Data quality checks failed for transaction data")
            
            target_type = transaction_config.get("target_type")

            if target_type == "redshift":
                self.redshift_loader.load_with_staging(
                    final_transactions,
                    transaction_config.get("target_path"),
                    key_columns=transaction_config.get("key_columns", ["transaction_id"])
                )
            elif target_type == "s3":
                self.s3_loader.write_delta(
                    final_transactions,
                    transaction_config.get("target_path"),
                    mode=transaction_config.get("write_mode", "append"),
                    partition_by=transaction_config.get("partition_cols", ["transaction_year", "transaction_month"])
                )
            elif target_type == "local":
                self.local_loader.write_delta(
                    final_transactions,
                    transaction_config.get("target_path"),
                    mode=transaction_config.get("write_mode", "append"),
                    partition_by=transaction_config.get("partition_cols", ["transaction_year", "transaction_month"])
                )
            else:
                self.logger.error(f"Unsupported target type: {target_type}")
                raise ValueError(f"Unsupported target type: {target_type}")
            
            self.logger.info("Transaction data pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in transaction data pipeline: {str(e)}")
    

    def run_customer_pipeline(self):
        """ Run the customer pipeline"""
        self.logger.info("Running the customer pipeline")

        try:
            customer_config = self.config.get("pipelines", {}).get("customer", {})
            source_type = customer_config.get("source_type")
            source_format = customer_config.get("source_format")

            if source_type == "s3":
                if source_format == "csv":
                    raw_customers = self.s3_connector.read_csv(
                        customer_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_customers = self.s3_connector.read_parquet(
                        customer_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_customers = self.s3_connector.read_json(
                        customer_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_customers = self.s3_connector.read_delta(
                        customer_config.get("source_path")
                    )
                else:
                    raise ValueError(f"Unsupported source format {source_format}")
            elif source_type == "rds":
                raw_customers = self.rds_connector.read_table(
                    customer_config.get("source_path")
                )
            elif source_type == "local":
                if source_format == "csv":
                    raw_customers = self.local_connector.read_csv(
                        customer_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_customers = self.local_connector.read_parquet(
                        customer_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_customers = self.local_connector.read_json(
                        customer_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_customers = self.local_connector.read_delta(
                        customer_config.get("source_path")
                    )
                else:
                    raise ValueError(f"Unsupported source format {source_format}")
            else:
                raise ValueError(f"Unsupported source type {source_type}")
            
            # Transform customer data
            cleaned_customers = self.customer_transformer.clean_customer_data(raw_customers)
            enriched_customers = self.customer_transformer.enrich_customer_data(cleaned_customers)

            # Run data quality checks
            quality_results = self.data_quality_checker.run_all_checks(
                enriched_customers,
                customer_config.get("data_quality", {})
            )

            if not quality_results.get("overall_passed", False):
                self.logger.warning("Data quality checks failed for customer data")
                if customer_config.get("fail_on_quality_check", True):
                    raise Exception("Data quality checks failed for customer data")
                
            target_type = customer_config.get("target_type")

            if target_type == "redshift":
                self.redshift_loader.load_with_staging(
                    enriched_customers,
                    customer_config.get("target_path"),
                    key_columns=customer_config.get("key_columns", ["customer_id"])
                )
            elif target_type == "s3":
                self.s3_loader.write_delta(
                    enriched_customers,
                    customer_config.get("target_path"),
                    mode=customer_config.get("write_mode"),
                    partition_by=customer_config.get("partition_cols"),
                    key_columns=customer_config.get("key_columns")
                )
            elif target_type == "local":
                self.local_loader.write_delta(
                    enriched_customers,
                    customer_config.get("target_path"),
                    mode=customer_config.get("write_mode"),
                    partition_by=customer_config.get("partition_cols"),
                    key_columns=customer_config.get("key_columns")
                )
            else:
                self.logger.error(f"Unsupported target type: {target_type}")
                raise ValueError(f"Unsupported target type: {target_type}")
            
            self.logger.info("Customer data pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in customer data pipeline: {str(e)}")

                
    def run_account_pipeline(self):
        """Run the account pipeline"""
        self.logger.info("Running the account pipeline")
        
        try:
            account_config = self.config.get("pipelines", {}).get("account", {})
            source_type = account_config.get("source_type")
            source_format = account_config.get("source_format")

            if source_type == "s3":
                if source_format == "csv":
                    raw_accounts = self.s3_connector.read_csv(
                        account_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_accounts = self.s3_connector.read_parquet(
                        account_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_accounts = self.s3_connector.read_json(
                        account_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_accounts = self.s3_connector.read_delta(
                        account_config.get("source_path")
                    )
                else:
                    raise ValueError(f"Unsupported source format {source_format}")
            elif source_type == "rds":
                raw_accounts = self.rds_connector.read_table(
                    account_config.get("source_path")
                )
            elif source_type == "local":
                if source_format == "csv":
                    raw_accounts = self.local_connector.read_csv(
                        account_config.get("source_path")
                    )
                elif source_format == "parquet":
                    raw_accounts = self.local_connector.read_parquet(
                        account_config.get("source_path")
                    )
                elif source_format == "json":
                    raw_accounts = self.local_connector.read_json(
                        account_config.get("source_path")
                    )
                elif source_format == "delta":
                    raw_accounts = self.local_connector.read_delta(
                        account_config.get("source_path")
                    )
                else:
                    raise ValueError(f"Unsupported source format {source_format}")
            else:
                raise ValueError(f"Unsupported source type {source_type}")
            
            # Transform account data
            cleaned_accounts = self.account_transformer.clean_account_data(raw_accounts)
            enriched_accounts = self.account_transformer.enrich_account_data(cleaned_accounts)

            # Run data quality checks
            quality_results = self.data_quality_checker.run_all_checks(
                enriched_accounts,
                account_config.get("data_quality", {})
            )

            if not quality_results.get("overall_passed", False):
                self.logger.warning("Data quality checks failed for account data")
                if account_config.get("fail_on_quality_check", True):
                    raise Exception("Data quality checks failed for account data")
                
            target_type = account_config.get("target_type")

            if target_type == "redshift":
                self.redshift_loader.load_with_staging(
                    enriched_accounts,
                    account_config.get("target_path"),
                    key_columns=account_config.get("key_columns", ["account_id"])
                )
            elif target_type == "s3":
                self.s3_loader.write_delta(
                    enriched_accounts,
                    account_config.get("target_path"),
                    mode=account_config.get("write_mode"),
                    partition_by=account_config.get("partition_cols"),
                    key_columns=account_config.get("key_columns")
                )
            elif target_type == "local":
                self.local_loader.write_delta(
                    enriched_accounts,
                    account_config.get("target_path"),
                    mode=account_config.get("write_mode"),
                    partition_by=account_config.get("partition_cols"),
                    key_columns=account_config.get("key_columns")
                )
            else:
                self.logger.error(f"Unsupported target type: {target_type}")
                raise ValueError(f"Unsupported target type: {target_type}")
            
            self.logger.info("Account data pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error in account data pipeline: {str(e)}")


    def run_pipeline(self):
        """ Run the ETL pipeline"""
        self.logger.info("Starting the banking ETL pipeline")

        try:
            pipelines_to_run = self.config.get("pipelines_to_run", [])

            if "customer" in pipelines_to_run:
                self.run_customer_pipeline()
            
            if "account" in pipelines_to_run:
                self.run_account_pipeline()
            
            if "transaction" in pipelines_to_run:
                self.run_transaction_pipeline()

            self.logger.info("Banking ETL pipeline completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error running the banking ETL pipeline: {str(e)}")
            raise
        finally:
            # Clean up resources
            self.logger.info("Cleaning up resources")
            self.spark.stop()


if __name__ == "__main__":
    config_path = os.environ.get("ETL_CONFIG_PATH", "config/config.json")

    # Run the pipeline
    pipeline = BankingETLPipeline(config_path)
    pipeline.run_pipeline()         




            

        


    




