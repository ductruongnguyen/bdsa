from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession

from utils.db_utils import insert_spark_df_to_postgres, read_data_from_postgres, \
    check_exist_records
from utils.singleton_spark import SparkSessionSingleton


def get_platform_pd(spark: SparkSession):
    platform_df = read_data_from_postgres(spark, "dim_platform")
    platform_pd = platform_df.select('platform_id').toPandas()
    return platform_pd


def insert_dim_date(spark: SparkSession):
    now_date = datetime.now()
    current_datetime = now_date.date()
    current_date_str = now_date.strftime('%Y-%m-%d')

    dim_date_df = read_data_from_postgres(spark, "dim_date")
    dim_date_pd = dim_date_df.select('date').toPandas()

    if current_datetime not in dim_date_pd['date'].values:
        # Dictionary to convert to DataFrame
        data_dict = {
            'date': [current_datetime],
            'day': [current_datetime.day],
            'month': [current_datetime.month],
            'year': [current_datetime.year],
            'quarter': [(current_datetime.month - 1) // 3 + 1]
        }

        # Create DataFrame from list of Row objects
        df = spark.createDataFrame(pd.DataFrame(data_dict))
        insert_spark_df_to_postgres(df, 'dim_date')
    else:
        print(f"The date {current_date_str} already exists in dim_date.")


def insert_dim_seller(spark: SparkSession):
    platform_pd = get_platform_pd(spark)

    for platform_id in platform_pd['platform_id'].values:
        sql_command = (f"SELECT EXISTS(SELECT 1 FROM ecommerce.dim_seller "
                       f"WHERE seller_code = -1 and platform_id = {platform_id})")
        exists = check_exist_records(sql_command)

        if exists:
            print(f"Seller with ID in platform- {platform_id} you are searching for exists in the database")
        else:
            # Dictionary to convert to DataFrame
            data_dict = {
                'seller_code': [-1],
                'seller_name': ['No Seller'],
                'seller_url': ['No url'],
                'platform_id': [platform_id]
            }

            # Create DataFrame from list of Row objects
            df = spark.createDataFrame(pd.DataFrame(data_dict))
            insert_spark_df_to_postgres(df, 'dim_seller')


def insert_dim_brand(spark: SparkSession):
    platform_pd = get_platform_pd(spark)

    for platform_id in platform_pd['platform_id'].values:
        sql_command = (f"SELECT EXISTS(SELECT 1 FROM ecommerce.dim_brand "
                       f"WHERE brand_code = -1 and platform_id = {platform_id})")
        exists = check_exist_records(sql_command)

        if exists:
            print(f"Brand with ID in platform- {platform_id} you are searching for exists in the database")
        else:
            # Dictionary to convert to DataFrame
            data_dict = {
                'brand_code': [-1],
                'brand_name': ['No Brand'],
                'platform_id': [platform_id]
            }

            # Create DataFrame from list of Row objects
            df = spark.createDataFrame(pd.DataFrame(data_dict))

            insert_spark_df_to_postgres(df, 'dim_brand')


def insert_dim_category(spark: SparkSession):
    platform_pd = get_platform_pd(spark)

    for platform_id in platform_pd['platform_id'].values:
        sql_command = (f"SELECT EXISTS(SELECT 1 FROM ecommerce.dim_category "
                       f"WHERE category_code = -1 and platform_id = {platform_id})")
        exists = check_exist_records(sql_command)

        if exists:
            print(f"Category with ID in platform- {platform_id} you are searching for exists in the database")
        else:
            # Dictionary to convert to DataFrame
            data_dict = {
                'category_code': [-1],
                'platform_id': [platform_id],
                'category_name': ['Khác'],
                'category_url': ['No url'],
                'level': [0]
            }

            # Create DataFrame from list of Row objects
            df = spark.createDataFrame(pd.DataFrame(data_dict))
            insert_spark_df_to_postgres(df, 'dim_category')


def insert_dim_location(spark: SparkSession):
    sql_command = (f"SELECT EXISTS(SELECT 1 FROM ecommerce.dim_location "
                   f"WHERE location_id = -1)")
    exists = check_exist_records(sql_command)

    if exists:
        print(f"Location with ID you are searching for exists in the database")
    else:
        # Dictionary to convert to DataFrame
        data_dict = {
            'location_id': [-1],
            'location_name': ['Chưa rõ']
        }

        # Create DataFrame from list of Row objects
        df = spark.createDataFrame(pd.DataFrame(data_dict))
        insert_spark_df_to_postgres(df, 'dim_location')


def main():
    spark = SparkSessionSingleton.get_instance()
    insert_dim_date(spark)
    insert_dim_seller(spark)
    insert_dim_brand(spark)
    insert_dim_category(spark)
    insert_dim_location(spark)


if __name__ == "__main__":
    main()
