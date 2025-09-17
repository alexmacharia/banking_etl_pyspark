from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

import logging

logger = logging.getLogger(__name__)

class TransformTransaction:
    """ Class to handle transactions transformations"""

    def __init__(self, spark: SparkSession):
        """Initialize the transactions transformation class
        
           Args:
               spark (SparkSession): Spark session
        """
        self.spark = spark

    
    def 