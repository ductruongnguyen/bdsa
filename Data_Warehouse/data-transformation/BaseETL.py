from pyspark.sql import DataFrame, SparkSession
import logging
import logging.config
from utils.db_utils import insert_spark_df_to_postgres
from typing import Any


class BaseETL:
    def __init__(self):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        self.logger = logging.getLogger(self.__class__.__name__)

    def process_data(self, spark: SparkSession, input_df: DataFrame | Any) -> Any:
        raise NotImplementedError("This method should be implemented by subclasses")

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        self.logger.info(f"Start writing {df.count()} records to {table_name}")
        insert_spark_df_to_postgres(df, table_name)
