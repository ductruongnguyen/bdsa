import json
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, trim, col, lit, when, upper, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DecimalType, \
    DateType

from BaseETL import BaseETL
from utils.config import PLATFORM
from utils.db_utils import read_data_from_postgres
from utils.etl_utils import extract_lazada_discount_udf, extract_sold_number_udf
from utils.s3_utils import get_most_recent_file_s3, read_file_from_s3
from utils.singleton_spark import SparkSessionSingleton
from datetime import datetime
from general_initial_load import insert_dim_date


def flatten_category_from_json(categories: List[Dict]) -> List:
    platform_id = PLATFORM['lazada']
    result = []

    def safe_cast(val, to_type, default=None):
        try:
            return to_type(val)
        except (ValueError, TypeError):
            return default

    for level_1 in categories:
        result.append({
            'category_code': safe_cast(level_1['id'], int, -1),
            'platform_id': platform_id,
            'category_name': str(level_1['categoryName']),
            'category_url': '',
            'parent_code': -1,
            'level': 0
        })
        if level_1.get('level2TabList', []):
            for level_2 in level_1['level2TabList']:
                result.append({
                    'category_code': safe_cast(level_2['categoryId'], int, -1),
                    'platform_id': platform_id,
                    'category_name': str(level_2['categoryName']),
                    'category_url': str(level_2['categoryUrl']),
                    'parent_code': int(level_1['id']),
                    'level': 1
                })
                if level_2.get('level3TabList', []):
                    for level_3 in level_2['level3TabList']:
                        result.append({
                            'category_code': safe_cast(level_3['categoryId'], int, -1),
                            'platform_id': platform_id,
                            'category_name': str(level_3['categoryName']),
                            'category_url': str(level_3['categoryUrl']),
                            'parent_code': int(level_2['categoryId']),
                            'level': 2
                        })
    return result


def extract_categories_df(spark: SparkSession, prefix: str, file_prefix: str) -> DataFrame | None:
    most_recent_category = get_most_recent_file_s3(prefix, file_prefix)
    if not most_recent_category:
        print("No recent category files found.")
        return None

    file_data = read_file_from_s3(most_recent_category['Key'])
    if not file_data:
        print("Failed to read category file data.")
        return None

    categories = flatten_category_from_json(json.loads(file_data))
    rows = [tuple(cat.values()) for cat in categories]

    schema = StructType([
        StructField("category_code", LongType(), nullable=False),
        StructField("platform_id", IntegerType(), nullable=False),
        StructField("category_name", StringType(), nullable=False),
        StructField("category_url", StringType(), nullable=True),
        StructField("parent_code", LongType(), nullable=True),
        StructField("level", IntegerType(), nullable=False)
    ])

    return spark.createDataFrame(rows, schema=schema)


class CategoryETL(BaseETL):
    def process_data(self, spark: SparkSession, dummy_df: DataFrame) -> DataFrame:
        categories_df = extract_categories_df(spark, 'lazada/raw/category', 'cat')
        return categories_df

    def write_data_to_postgres(self, spark: SparkSession, categories_df: DataFrame, table_name: str):
        lazada_id = PLATFORM['lazada']
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
            super().write_data_to_postgres(
                spark,
                level_df_insert,
                "dim_category"
            )

            # Read the inserted categories back to get the auto-incremented IDs
            inserted_df = read_data_from_postgres(spark, table_name).filter((col("level") == lit(level)) &
                                                                            (col("platform_id") == lit(lazada_id)))

            # Collect inserted categories
            inserted_categories_df = inserted_categories_df.union(inserted_df)


class SellerETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        seller_df = input_df.select(
            trim(input_df.product.sellerId).cast('bigint').alias('seller_code'),
            trim(input_df.product.sellerName).alias('seller_name'),
            input_df.platform_id.cast('int').alias('platform_id')
        ).dropna(subset=["seller_code"]).dropDuplicates(["seller_code"])

        return seller_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class LocationETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        location_df = (input_df.select(
            upper(trim(input_df.product.location)).alias('location_name')
        ).dropna(subset=["location_name"]).dropDuplicates(["location_name"]))

        return location_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class BrandETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        brand_df = input_df.select(
            trim(input_df.product.brandId).cast('bigint').alias('brand_code'),
            trim(input_df.product.brandName).alias('brand_name'),
            input_df.platform_id.cast('int').alias('platform_id')
        ).dropna(subset=["brand_code"]).dropDuplicates(["brand_code"])

        return brand_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class ProductETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        lazada_id = PLATFORM['lazada']
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(lazada_id))).select(
                'seller_id').collect()[0]['seller_id']

        specific_date = '2024-07-21'  # todo: incase wanna set a specific date

        products_df = (input_df.select(
            trim(input_df.product.itemId).alias('product_code'),
            trim(col('product.name')).alias('product_name'),
            trim(col('product.name')).alias('product_description'),
            trim(input_df.product.image).alias('product_image'),
            trim(input_df.product.itemUrl).alias('product_url'),

            coalesce(trim(input_df.product.price).cast(DecimalType(15, 2)), lit(0.00)).alias('price'),
            coalesce(trim(input_df.product.originalPrice).cast(DecimalType(15, 2)), lit(0.00)).alias(
                'original_price'),
            coalesce(extract_lazada_discount_udf(trim(input_df.product.discount)).cast(DecimalType(5, 2)),
                     lit(0.00)).alias('discount_rate'),
            coalesce(trim(input_df.product.ratingScore).cast(DecimalType(5, 2)), lit(0.00)).alias(
                'rating_score'),
            coalesce(trim(input_df.product.review).cast(IntegerType()), lit(0)).alias('review_count'),

            coalesce(trim(input_df.product.sellerId).cast(LongType()), lit(-1)).alias('seller_code'),
            input_df.platform_id.cast('int').alias('platform_id'),

            coalesce(extract_sold_number_udf(trim(input_df.product.itemSoldCntShow)).cast(LongType()),
                     lit(0)).alias('sold'),
            # current_date().cast('date').alias('effective_date'),
            lit(specific_date).cast(DateType()).alias('effective_date'),  # todo: change if required
            lit(None).cast("date").alias('expired_date'),
            lit(True).alias('flag')
        ).dropna(subset=["product_code"]).drop_duplicates(subset=['product_code', 'platform_id', 'seller_code']))

        df_products_sellers = products_df.alias("p").join(
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
        lazada_id = PLATFORM['lazada']
        dim_category = read_data_from_postgres(spark, 'dim_category')
        dim_brand = read_data_from_postgres(spark, 'dim_brand')
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        dim_location = read_data_from_postgres(spark, 'dim_location')
        dim_product = read_data_from_postgres(spark, 'dim_product')
        dim_product = dim_product.filter((col('platform_id') == lit(lazada_id)) &
                                         (col('flag') == lit(True)))

        default_category_id = \
            dim_category.filter(
                (col('category_code') == lit(-1)) & (col('platform_id') == lit(lazada_id))).select(
                'category_id').collect()[0]['category_id']
        default_brand_id = \
            dim_brand.filter((col('brand_code') == lit(-1)) & (col('platform_id') == lit(lazada_id))).select(
                'brand_id').collect()[0]['brand_id']
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(lazada_id))).select(
                'seller_id').collect()[0]['seller_id']

        fact_sales_df = input_df.select(
            lit(lazada_id).cast('int').alias('platform_id'),
            col('category_id').cast('bigint').alias('category_code'),
            col('product.brandId').cast('bigint').alias('brand_code'),
            col('product.sellerId').cast('bigint').alias('seller_code'),
            col('product.itemId').alias('product_code'),
            upper(trim(col('product.location'))).alias('location_name')
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

        # Join with dim_location to get location_id
        fact_sales_df = fact_sales_df.alias("f").join(
            dim_location.alias("l"),
            on=['location_name'],
            how='left'
        ).withColumn(
            'location_id',
            when(col('l.location_id').isNotNull(), col('l.location_id')).otherwise(lit(-1))
        ).drop('l.location_name')  # Drop duplicate columns after join

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
    lazada_data_path = "./data/lazada/1/*.json"  # todo: change if required
    lazada_data = spark.read.json(lazada_data_path)
    flat_data = lazada_data.select(
        explode(lazada_data.products).alias("product"),
        trim(lazada_data.main_info.trackParams.category).cast('bigint').alias('category_id'),
        lit(PLATFORM['lazada']).alias('platform_id')
    )

    # category = CategoryETL()
    # category_df_final = category.process_data(spark, flat_data)
    # category.write_data_to_postgres(spark, category_df_final, 'dim_category')
    #
    # seller = SellerETL()
    # seller_df = seller.process_data(spark, flat_data)
    # seller.write_data_to_postgres(spark, seller_df, 'dim_seller')
    #
    # location = LocationETL()
    # location_df = location.process_data(spark, flat_data)
    # location.write_data_to_postgres(spark, location_df, 'dim_location')
    #
    # brand = BrandETL()
    # brand_df = brand.process_data(spark, flat_data)
    # brand.write_data_to_postgres(spark, brand_df, 'dim_brand')

    product = ProductETL()
    dim_product = product.process_data(spark, flat_data)
    product.write_data_to_postgres(spark, dim_product, 'dim_product')

    fact_sale = FactSaleETL()
    fact_sale_df = fact_sale.process_data(spark, flat_data)
    fact_sale.write_data_to_postgres(spark, fact_sale_df, "fact_sales")


if __name__ == "__main__":
    runner()
