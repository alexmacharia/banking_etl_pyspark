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
        local_config = self.config.get("local", {})
        self.local_connector = LocalConnector(
            self.spark,
            local_config.get("base_data_path", "")
        )

        # Initialize transformers
        self.transaction_transformer = TransactionTransformer(self.spark)

        # Initialize data quality checker
        self.data_quality_cheker = DataQualityChecker(self.spark)

        


    




