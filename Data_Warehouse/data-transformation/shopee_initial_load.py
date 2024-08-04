import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (explode, trim, col, lit, udf, current_date, upper, split,
                                   regexp_replace, element_at, regexp_extract, when, coalesce)
from pyspark.sql.types import DecimalType, FloatType, IntegerType, DateType, StructType, StructField, LongType, StringType

from BaseETL import BaseETL
from utils.db_utils import read_data_from_postgres
from utils.etl_utils import extract_shopee_location_udf, extract_sold_number_udf
from utils.singleton_spark import SparkSessionSingleton
from utils.config import PLATFORM
from datetime import datetime
from general_initial_load import insert_dim_date


class CategoryETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        category_df = input_df.select(explode("category").alias("category"))
        category_df = category_df.select(
            col("category.category_id").cast('bigint').alias("category_code"),
            lit(10).alias("platform_id"),
            col("category.category_name").alias("category_name"),
            lit('No url').alias("category_url"),
            col("category.parent_id").cast('bigint').alias("parent_code"),
            when(col("category.parent_id") == lit(-1), lit(0)).otherwise(lit(1)).alias('level')
        ).dropna(subset=["category_code"]).drop_duplicates(["category_code"])

        return category_df

    def write_data_to_postgres(self, spark: SparkSession, categories_df: DataFrame, table_name: str):
        shoppe_id = PLATFORM['shopee']
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
                table_name
            )

            # Read the inserted categories back to get the auto-incremented IDs
            inserted_df = read_data_from_postgres(spark, table_name).filter((col("level") == lit(level)) &
                                                                            (col("platform_id") == lit(shoppe_id)))

            # Collect inserted categories
            inserted_categories_df = inserted_categories_df.union(inserted_df)


class LocationETL(BaseETL):
    def process_data(self, spark: SparkSession, data_df: DataFrame) -> DataFrame:
        location_df = (data_df.select(upper(extract_shopee_location_udf(trim(col('location')))).alias('location_name'))
                       .dropna().drop_duplicates())
        location_name_input = location_df.toPandas()

        location_db = read_data_from_postgres(spark, 'dim_location')
        location_name_db = location_db.select('location_name').toPandas()

        new_locations = [location for location in location_name_input['location_name'].values if
                         location not in location_name_db['location_name'].values]

        # check if the list is empty before creating the DataFrame
        if not new_locations:
            new_location_df = spark.createDataFrame(spark.sparkContext.emptyRDD(),
                                                    StructType([StructField('location_name', StringType())]))
        else:
            new_location_df = spark.createDataFrame(pd.DataFrame(new_locations, columns=['location_name']))

        return new_location_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class ProductETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        shoppe_id = PLATFORM['shopee']
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(shoppe_id))).select(
                'seller_id').collect()[0]['seller_id']

        specific_date = '2024-07-21'  # todo: incase wanna set a specific date

        df_products = (input_df.select(
            split(regexp_replace(regexp_extract(col("url"), r'i\.\d+\.\d+\?', 0), r'\?', ''), r'\.').getItem(2).alias(
                'product_code'),
            col('title').alias('product_name'),
            col('title').alias('product_description'),
            col('image').alias('product_image'),
            col('url').alias('product_url'),

            coalesce(
                regexp_replace(col("price.current_price"), "\\.", "").cast(DecimalType(15, 2)),
                lit(0.00)
            ).alias('price'),
            coalesce(
                regexp_replace(col("price.original_price"), "[₫.]", "").cast(DecimalType(15, 2)),
                lit(0.00)
            ).alias('original_price'),
            coalesce(
                (regexp_replace(col("price.discount_rate"), "[-%]", "").cast(FloatType()) / 100).cast(
                    DecimalType(5, 2)),
                lit(0.00)
            ).alias('discount_rate'),
            lit(0.00).cast(DecimalType(5, 2)).alias('rating_score'),
            lit(0).cast('int').alias('review_count'),

            lit(default_seller_id).cast('bigint').alias('seller_id'),
            lit(shoppe_id).cast('int').alias('platform_id'),

            coalesce(
                extract_sold_number_udf(trim(col("sold"))).cast('bigint'),
                lit(0)
            ).alias('sold'),
            # current_date().cast('date').alias('effective_date'),
            lit(specific_date).cast(DateType()).alias('effective_date'),  # todo: change if required
            lit(None).cast("date").alias('expired_date'),
            lit(True).alias('flag')
        ).dropna(subset=["product_code"]).drop_duplicates(subset=['product_code', 'platform_id', 'seller_id']))

        df_products_return = spark.createDataFrame(df_products.rdd, df_products.schema)
        return df_products_return

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class FactSaleETL(BaseETL):
    def process_data(self, spark: SparkSession, input_df: DataFrame) -> DataFrame:
        shoppe_id = PLATFORM['shopee']
        dim_category = read_data_from_postgres(spark, 'dim_category')
        dim_brand = read_data_from_postgres(spark, 'dim_brand')
        dim_seller = read_data_from_postgres(spark, 'dim_seller')
        dim_location = read_data_from_postgres(spark, 'dim_location')
        dim_product = read_data_from_postgres(spark, 'dim_product')
        dim_product = dim_product.filter((col('platform_id') == lit(shoppe_id)) &
                                         (col('flag') == lit(True)))

        default_category_id = \
            dim_category.filter(
                (col('category_code') == lit(-1)) & (col('platform_id') == lit(shoppe_id))).select(
                'category_id').collect()[0]['category_id']
        default_brand_id = \
            dim_brand.filter((col('brand_code') == lit(-1)) & (col('platform_id') == lit(shoppe_id))).select(
                'brand_id').collect()[0]['brand_id']
        default_seller_id = \
            dim_seller.filter((col('seller_code') == lit(-1)) & (col('platform_id') == lit(shoppe_id))).select(
                'seller_id').collect()[0]['seller_id']

        fact_sales_df = input_df.select(
            lit(shoppe_id).cast('int').alias('platform_id'),
            element_at(col('category.category_id'), 2).cast('bigint').alias('category_code'),
            lit(default_brand_id).cast('bigint').alias('brand_id'),
            lit(default_seller_id).cast('bigint').alias('seller_id'),
            split(regexp_replace(regexp_extract(col("url"), r'i\.\d+\.\d+\?', 0), r'\?', ''), r'\.').getItem(2).alias(
                'product_code'),
            upper(extract_shopee_location_udf(trim(col('location')))).alias('location_name')
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
    shopee_data_path = "./data/shopee/1/*.json"
    shopee_data = (spark.read
                   .option("multiline", "true")
                   .json(shopee_data_path))

    # category = CategoryETL()
    # category_df = category.process_data(spark, shopee_data)
    # if category_df.count() > 0:
    #     category.write_data_to_postgres(spark, category_df, 'dim_category')
    #
    # location = LocationETL()
    # location_df = location.process_data(spark, shopee_data)
    # if location_df.count() > 0:
    #     location.write_data_to_postgres(spark, location_df, 'dim_location')

    product = ProductETL()
    dim_product = product.process_data(spark, shopee_data)
    if dim_product.count() > 0:
        product.write_data_to_postgres(spark, dim_product, 'dim_product')

    fact_sale = FactSaleETL()
    face_sale_df = fact_sale.process_data(spark, shopee_data)
    if face_sale_df.count() > 0:
        fact_sale.write_data_to_postgres(spark, face_sale_df, 'fact_sales')


if __name__ == "__main__":
    runner()
