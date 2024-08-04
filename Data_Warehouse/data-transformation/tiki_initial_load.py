from datetime import datetime
from typing import Dict, List

import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, col, lit, current_date, when, coalesce
from pyspark.sql.types import DecimalType, FloatType, DateType

from BaseETL import BaseETL
from general_initial_load import insert_dim_date
from utils.config import PLATFORM
from utils.db_utils import insert_spark_df_to_postgres, read_data_from_postgres
from utils.s3_utils import S3_CLIENT, BUCKET_NAME
from utils.s3_utils import get_most_recent_file_s3
from utils.singleton_spark import SparkSessionSingleton
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType


def flatten_category(json_data, parent_id=-1, level=0, platform_id=PLATFORM['tiki']):
    categories = []
    for category in json_data:
        category_entry = {
            "category_code": int(category["category_id"]),
            "platform_id": platform_id,
            "category_name": category["category_name"],
            "category_url": category.get("category_urlKey", ""),
            "parent_code": parent_id,
            "level": level
        }
        categories.append(category_entry)
        if "children" in category:
            categories.extend(flatten_category(category["children"],
                                               int(category["category_id"]),
                                               level + 1,
                                               platform_id)
                              )
    return categories


def extract_category_df(spark: SparkSession, categories: List[Dict]) -> DataFrame:
    schema = StructType([
        StructField("category_code", LongType(), nullable=False),
        StructField("platform_id", IntegerType(), nullable=False),
        StructField("category_name", StringType(), nullable=False),
        StructField("category_url", StringType(), nullable=True),
        StructField("parent_code", LongType(), nullable=True),
        StructField("level", IntegerType(), nullable=False)
    ])

    flattened_categories = flatten_category(categories)
    rows = [tuple(cat.values()) for cat in flattened_categories]

    categories_df = spark.createDataFrame(rows, schema=schema)
    return categories_df


class CategoryETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        most_recent_cat = get_most_recent_file_s3('tiki/raw/category', 'cat')

        file_data = None
        try:
            file_obj = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=most_recent_cat['Key'])
            file_data = file_obj["Body"].read().decode('utf-8')
            print(f'File {most_recent_cat["Key"]} is read successfully')
        except Exception as e:
            print(e)

        categories_df = extract_category_df(spark, json.loads(file_data))
        return categories_df

    def write_data_to_postgres(self, spark: SparkSession, categories_df: DataFrame, table_name: str):
        tiki_id = PLATFORM['tiki']
        schema = StructType([
            StructField("category_id", IntegerType(), nullable=False),
            StructField("category_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("category_name", StringType(), nullable=False),
            StructField("category_url", StringType(), nullable=True),
            StructField("parent_id", IntegerType(), nullable=True),
            StructField("level", IntegerType(), nullable=False)
        ])

        inserted_categories_df = spark.createDataFrame([], schema=schema)

        for level in range(categories_df.select("level").distinct().count()):
            level_df = categories_df.filter(col("level") == lit(level))

            # Join with inserted categories to find parent_id
            if level > 0:
                level_df = level_df.alias('new').join(
                    inserted_categories_df.select(
                        col("category_code").alias("parent_code"),
                        col("category_id").alias("parent_id")
                    ).alias('existing'),
                    on="parent_code",
                    how="left"
                ).select(
                    col("new.category_code"),
                    col("new.platform_id"),
                    col("new.category_name"),
                    col("new.category_url"),
                    col("existing.parent_id"),
                    col("new.level")
                )
            else:
                level_df = level_df.withColumn("parent_id", lit(None))

            level_df = level_df.select("category_code",
                                       "platform_id",
                                       "category_name",
                                       "category_url",
                                       "parent_id",
                                       "level")

            # Write the level DataFrame to PostgreSQL and retrieve inserted categories with IDs
            level_df_insert = spark.createDataFrame(level_df.rdd, level_df.schema)
            level_df_insert = level_df_insert.withColumn("parent_id", col("parent_id").cast("Integer"))
            insert_spark_df_to_postgres(
                level_df_insert,
                "dim_category"
            )

            # Read the inserted categories back to get the auto-incremented IDs
            inserted_df = read_data_from_postgres(spark, table_name).filter((col("level") == lit(level)) &
                                                                            (col("platform_id") == lit(tiki_id)))

            # Collect inserted categories
            inserted_categories_df = inserted_categories_df.union(inserted_df)


class BrandETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        df_brands = input_df.select(trim(col('data.brand.id')).cast('bigint').alias('brand_code'),
                                    trim(col('data.brand.name')).alias('brand_name'),
                                    lit(PLATFORM['tiki']).alias('platform_id')
                                    ).dropna(subset=['brand_code']).drop_duplicates(subset=['brand_code'])

        return df_brands

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class SellerETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        df_sellers = (input_df.select(trim(col('data.current_seller.id')).cast('bigint').alias('seller_code'),
                                      trim(col('data.current_seller.name')).alias('seller_name'),
                                      lit(PLATFORM['tiki']).alias('platform_id'),
                                      trim(col('data.current_seller.link')).alias('seller_url')
                                      ).dropna(subset=['seller_code']).drop_duplicates(subset=['seller_code']))

        return df_sellers

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class ProductETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(PLATFORM['tiki']))).select(
                'seller_id').collect()[0]['seller_id']

        specific_date = '2024-07-21'  # todo: incase wanna set a specific date

        df_products = (input_df.select(
            col('product_id').alias('product_code'),
            col('data.name').alias('product_name'),
            col('data.short_description').alias('product_description'),
            col('data.thumbnail_url').alias('product_image'),
            col('data.short_url').alias('product_url'),

            coalesce(col('data.price').cast(DecimalType(15, 2)), lit(0.00)).alias('price'),
            coalesce(col('data.original_price').cast(DecimalType(15, 2)), lit(0.00)).alias('original_price'),
            coalesce((col('data.discount_rate').cast(FloatType()) / 100).cast(DecimalType(5, 2)), lit(0.00)).alias(
                'discount_rate'),
            coalesce(col('data.rating_average').cast(DecimalType(5, 2)), lit(0.00)).alias('rating_score'),
            coalesce(col('data.review_count').cast('int'), lit(0)).alias('review_count'),

            coalesce(col('data.current_seller.id').cast('bigint'), lit(-1)).alias('seller_code'),
            lit(PLATFORM['tiki']).cast('int').alias('platform_id'),

            coalesce(col('data.all_time_quantity_sold').cast('bigint'), lit(0)).alias('sold'),
            # current_date().alias('effective_date'),
            lit(specific_date).cast(DateType()).alias('effective_date'),  # todo: change if required
            lit(None).cast("date").alias('expired_date'),
            lit(True).alias('flag')
        ).dropna(subset=["product_code"]).drop_duplicates(subset=['product_code', 'platform_id', 'seller_code']))

        df_products_sellers = df_products.alias("p").join(
            dim_seller.alias("s"),
            on=['seller_code', 'platform_id'],
            how='left'
        )

        df_products_final = df_products_sellers.select(
            'p.product_code',
            'p.product_name',
            'p.product_description',
            'p.product_image',
            'p.product_url',
            'p.price',
            'p.original_price',
            'p.discount_rate',
            'p.rating_score',
            'p.review_count',
            'p.platform_id',
            'p.sold',
            'p.effective_date',
            'p.expired_date',
            'p.flag',
            when(col('s.seller_id').isNotNull(), col('s.seller_id'))
            .otherwise(lit(default_seller_id)).alias('seller_id')
        )

        df_products_return = spark.createDataFrame(df_products_final.rdd, df_products_final.schema)

        return df_products_return

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class FactSaleETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        tiki_platform_id = PLATFORM['tiki']
        dim_category = read_data_from_postgres(spark, 'dim_category')
        dim_brand = read_data_from_postgres(spark, 'dim_brand')
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        dim_product = read_data_from_postgres(spark, 'dim_product')
        dim_product = dim_product.filter((col('platform_id') == lit(tiki_platform_id)) &
                                         (col('flag') == lit(True)))

        default_category_id = \
            dim_category.filter(
                (col('category_code') == lit(-1)) & (col('platform_id') == lit(tiki_platform_id))).select(
                'category_id').collect()[0]['category_id']
        default_brand_id = \
            dim_brand.filter((col('brand_code') == lit(-1)) & (col('platform_id') == lit(tiki_platform_id))).select(
                'brand_id').collect()[0]['brand_id']
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(tiki_platform_id))).select(
                'seller_id').collect()[0]['seller_id']

        fact_sales_df = input_df.select(
            lit(tiki_platform_id).cast('int').alias('platform_id'),
            col('category_id').alias('category_code'),
            col('data.brand.id').alias('brand_code'),
            col('data.current_seller.id').alias('seller_code'),
            col('product_id').alias('product_code'),
            lit(-1).alias('location_id')
        )

        # Join with dim_category to get category_id
        fact_sales_df = fact_sales_df.alias("f").join(
            dim_category.alias("c"),
            on=['category_code', 'platform_id'],
            how='left'
        ).withColumn(
            'category_id',
            when(col('c.category_id').isNotNull(), col('c.category_id')).otherwise(lit(default_category_id))
        ).drop('c.category_code', 'c.platform_id')  # Drop duplicate columns after join

        # Join with dim_brand to get brand_id
        fact_sales_df = fact_sales_df.alias("f").join(
            dim_brand.alias("b"),
            on=['brand_code', 'platform_id'],
            how='left'
        ).withColumn(
            'brand_id',
            when(col('b.brand_id').isNotNull(), col('b.brand_id')).otherwise(lit(default_brand_id))
        ).drop('b.brand_code', 'b.platform_id')  # Drop duplicate columns after join

        # Join with dim_seller to get seller_id
        fact_sales_df = fact_sales_df.alias("f").join(
            dim_seller.alias("s"),
            on=['seller_code', 'platform_id'],
            how='left'
        ).withColumn(
            'seller_id',
            when(col('s.seller_id').isNotNull(), col('s.seller_id')).otherwise(lit(default_seller_id))
        ).drop('s.seller_code', 's.platform_id')  # Drop duplicate columns after join

        # Join with dim_product to get product_id
        fact_sales_df = fact_sales_df.alias("f").join(
            dim_product.alias("p"),
            on=['product_code', 'platform_id', 'seller_id'],
            how='inner'
        ).withColumn(
            'product_id', col("p.product_id")
        ).withColumn(
            'units_sold', col("p.sold")
        ).withColumn(
            'total_sales_amount', col("p.sold") * col("p.price")
        ).drop('p.product_code', 'p.platform_id', 'p.seller_id')

        # Get the current date_id
        dim_date_df = read_data_from_postgres(spark, "dim_date")
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        filtered_df = dim_date_df.filter(dim_date_df.date == current_date_str).select("date_id")

        if not filtered_df.rdd.isEmpty():
            date_id = filtered_df.collect()[0][0]
        else:
            print("No matching date found in dim_date. Start importing dim_date")
            insert_dim_date(spark)
            dim_date_df = read_data_from_postgres(spark, "dim_date")
            current_date_str = datetime.now().strftime('%Y-%m-%d')
            date_id = dim_date_df.filter(dim_date_df.date == current_date_str).select("date_id").collect()[0][0]

        # fact_sales_df = fact_sales_df.withColumn("date_id", lit(date_id))  # todo: fix this date to the appropriate date
        fact_sales_df = fact_sales_df.withColumn("date_id", lit(3))

        fact_sales_df = fact_sales_df.select(
            col('date_id'),
            col('platform_id'),
            col('category_id'),
            col('brand_id'),
            col('seller_id'),
            col('product_id'),
            col('product_code'),
            col('location_id'),
            col('units_sold'),
            col('total_sales_amount')
        ).dropna(subset=["product_code"]).drop_duplicates(subset=['product_code', 'platform_id', 'seller_id'])
        fact_sales_returned = spark.createDataFrame(fact_sales_df.rdd, fact_sales_df.schema)

        return fact_sales_returned

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


def runner():
    spark = SparkSessionSingleton.get_instance()
    tiki_data_path = "./data/tiki/1/*.json"  # todo: change if required
    tiki_data = spark.read.json(tiki_data_path)

    category = CategoryETL()
    category_df = category.process_data(spark, tiki_data)
    category.write_data_to_postgres(spark, category_df, 'dim_category')

    brand = BrandETL()
    brand_df = brand.process_data(spark, tiki_data)
    brand.write_data_to_postgres(spark, brand_df, 'dim_brand')

    seller = SellerETL()
    seller_df = seller.process_data(spark, tiki_data)
    seller.write_data_to_postgres(spark, seller_df, 'dim_seller')

    product = ProductETL()
    dim_product = product.process_data(spark, tiki_data)
    product.write_data_to_postgres(spark, dim_product, 'dim_product')

    fact_sale = FactSaleETL()
    fact_sale_df = fact_sale.process_data(spark, tiki_data)
    fact_sale.write_data_to_postgres(spark, fact_sale_df, "fact_sales")


if __name__ == "__main__":
    runner()
