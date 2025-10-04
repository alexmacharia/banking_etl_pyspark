from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class RedshiftLoader:
    """ Class to handle data loading to AWS Redshift"""
    def __init__(self, spark: SparkSession, jdbc_url:str, username: str, password: str):
        """ 
        Initialize the Redshift loader class

        Args:
            spark (SparkSession): Spark session
            jdbc_url (str): Connection string for Redshift
            username (str): Redshift username
            password (str): Redshift password
        """
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password

    
    def write_to_redshift(self, df: DataFrame, table_name: str, write_mode: str = "append",
                          preactions: Optional[str] = None, postactions: Optional[str] = None) -> None:
        """
        Write DataFrame to Redshift table

        Args:
            df (DataFrame): DataFrame to write
            table_name (str): Target table name in Redshift
            write_mode (str): Write mode (append, overwrite, error)
            preactions (Optional[str]): SQL to execute before writing
            postactions (Optional[str]): SQL to execute after writing
        """
        try:
            logger.info(f"Writing data to Redshift table: {table_name}")

            connection_properties = {
                "url": self.jdbc_url,
                "user": self.username,
                "password": self.password,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "dbtable": table_name
            }

            if preactions:
                connection_properties["preactions"] = preactions
            if postactions:
                connection_properties["postactions"] = postactions

            df.write.format("jdbc") \
                .mode(write_mode) \
                .options(**connection_properties) \
                .save()
            
            logger.info(f"Successfully wrote data to Redshift table: {table_name}")
        except Exception as e:
            logger.error(f"Error writing to Redshift table {table_name}: {str(e)}")
            raise

    
    def load_with_staging(self, df:DataFrame, target_table:str, staging_table: str = None,
                           key_columns: List[str] = None) -> None:
        """" 
        Load data using a temporary staging table

        Args:
            df (DataFrame): Spark dataframe to be loaded
            target_table (str): Target Redshift table name
            staging_table (str): Staging table name
            key_columns (List[str]): Primary key column for merging
        """
        try:
            if not staging_table:
                staging_table = f"stg_{target_table}"

            logger.info(f"Loading data to Redshift table: {target_table} using staging table: {staging_table}")

            create_staging_sql =  f""" 
                DROP TABLE IF EXISTS {staging_table};
                CREATE TABLE {staging_table} (LIKE {target_table});
                """
            
            self.write_to_redshift(df, staging_table, "overwrite", preactions=create_staging_sql)

            if key_columns and len(key_columns) > 0:
                key_conditions = " AND ".join(f"target.{col} = source.{col}" for col in key_columns)
                non_key_columns = [col for col in df.columns if col not in key_columns]
                update_statements = ", ".join([f"target.{col} = source.{col}" for col in non_key_columns])
                insert_columns = ", ".join(df.columns)
                insert_values = ", ".join([f"source.{col}" for col in df.columns])

                merge_sql = f""" 
                    BEGIN TRANSACTION;

                    ----Update existing records
                    UPDATE {target_table} AS target
                    SET {update_statements}
                    FROM {staging_table} AS source
                    WHERE {key_conditions};
                    
                
                    ---Insert new records
                    INSERT INTO {target_table} ({insert_columns})
                    SELECT {insert_values}
                    FROM {staging_table} AS source
                    LEFT JOIN {target_table} AS target
                    ON {key_conditions}
                    WHERE target.{key_columns[0]} IS NULL;

                    ---Clean up staging table
                    DROP TABLE IF EXISTS {staging_table};

                    END TRANSACTION;
                    """
                
                self.execute_sql(merge_sql)
            else:
                truncate_and_load_sql = f""" 
                BEGIN TRANSACTION;

                TRUNCATE TABLE {target_table};

                INSERT INTO {target_table}
                SELECT * FROM {staging_table};

                DROP TABLE IF EXISTS {staging_table};

                END TRANSACTION;
                """

                self.execute_sql(truncate_and_load_sql)

            logger.info(f"Successfully loaded data to Redshift table: {target_table}")
        except Exception as e:
            logger.error(f"Error loading data to Redshift table {target_table}: {str(e)}")
            raise

    
    def execute_sql(self, sql: str) -> None:
        """ 
        Execute sql statement in Redshift

        Args:
            sql (str): SQL statement to execute
        """
        try:
            logger.info("Executing SQL in Redshift")

            temp_df = self.spark.createDataFrame([("dummy",)], ["dummy"])

            connection_properties = {
                "url": self.jdbc_url,
                "user": self.username,
                "password": self.password,
                "driver": "com.amazon.redshift.jdbc42.Driver",
                "dbtable": "(SELECT 1) AS dummy",
                "postactions": sql
            }

            temp_df.write.format("jdbc") \
                .mode("append") \
                .options(**connection_properties) \
                .save()
            
            logger.info("Successfully executed SQL in Redshift")
        except Exception as e:
            logger.error(f"Error executing SQL in Redshift: {str(e)}")
            raise