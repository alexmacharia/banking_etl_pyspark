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

        #Setup logging
        setup_logging()

        


