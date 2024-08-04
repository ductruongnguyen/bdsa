import time

import psycopg2
from psycopg2 import sql
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import logging
from utils.config import DB_CREDENTIALS

logger = logging.getLogger(__name__)


def insert_spark_df_to_postgres(spark_df: DataFrame, table_name: str) -> None:
    start_insert = time.time()

    jdbc_url = (
        f"jdbc:postgresql://{DB_CREDENTIALS['host']}:{DB_CREDENTIALS['port']}/{DB_CREDENTIALS['dbname']}"
    )

    connection_properties = {
        "user": DB_CREDENTIALS['username'],
        "password": DB_CREDENTIALS['password'],
        "driver": DB_CREDENTIALS['driver'],
        "connectTimeout": DB_CREDENTIALS['connectTimeout'],
        "socketTimeout": DB_CREDENTIALS['socketTimeout']
    }

    spark_df.write.jdbc(
        url=jdbc_url,
        table=f"{DB_CREDENTIALS['schema']}.{table_name}",
        mode="append",
        properties=connection_properties
    )

    end_insert = time.time()
    logger.info(f"Write Dataframe success to {table_name.capitalize()} time in {end_insert - start_insert} seconds")


def read_data_from_postgres(spark: SparkSession, table_name: str) -> DataFrame:
    start_time = time.time()
    jdbc_url = f"jdbc:postgresql://{DB_CREDENTIALS['host']}:{DB_CREDENTIALS['port']}/{DB_CREDENTIALS['dbname']}"
    connection_properties = {
        "user": DB_CREDENTIALS['username'],
        "password": DB_CREDENTIALS['password'],
        "driver": DB_CREDENTIALS['driver']
    }
    end_time = time.time()
    result = spark.read.jdbc(
        url=jdbc_url,
        table=f"{DB_CREDENTIALS['schema']}.{table_name}",
        properties=connection_properties
    )
    logger.info(f"Read data from {table_name.capitalize()} success in {end_time - start_time} seconds")
    return result


def check_exist_records(sql_command):
    # Database connection parameters
    db_params = {
        'dbname': DB_CREDENTIALS['dbname'],
        'user': DB_CREDENTIALS['username'],
        'password': DB_CREDENTIALS['password'],
        'host': DB_CREDENTIALS['host'],
        'port': DB_CREDENTIALS['port']
    }

    try:
        start_time = time.time()
        # Establish the connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Execute the SQL command
        cursor.execute(sql.SQL(sql_command))

        # Fetch the result
        result = cursor.fetchone()[0]

        # Close the connection
        cursor.close()
        conn.close()
        end_time = time.time()
        logger.info(f"Check exist records query executed successfully in {end_time - start_time} seconds.")
        return result

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return None


def update_existing_records(sql_command: str) -> None:
    db_params = {
        'dbname': DB_CREDENTIALS['dbname'],
        'user': DB_CREDENTIALS['username'],
        'password': DB_CREDENTIALS['password'],
        'host': DB_CREDENTIALS['host'],
        'port': DB_CREDENTIALS['port']
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    try:
        start_time = time.time()
        conn.autocommit = False

        # Prepare and execute the update query
        update_query = sql.SQL(sql_command)
        cursor.execute(update_query)

        # Commit the transaction if successful
        conn.commit()
        end_time = time.time()
        logger.info(f"Update query executed successfully in {end_time - start_time} seconds.")
    except Exception as error:
        if conn is not None:
            conn.rollback()
        logger.error(f"Error when executing update query: {error}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
