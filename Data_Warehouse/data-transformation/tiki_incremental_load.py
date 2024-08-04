from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, row_number, expr, coalesce, when
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from BaseETL import BaseETL
from tiki_initial_load import FactSaleETL as InitialFactSale
from tiki_initial_load import CategoryETL as InitialCategory, BrandETL as InitialBrand, \
    SellerETL as InitialSeller, ProductETL as InitialProduct
from utils.db_utils import read_data_from_postgres, update_existing_records
from utils.singleton_spark import SparkSessionSingleton
from utils.config import PLATFORM


class CategoryIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, new_categories_df: DataFrame) -> Any:
        tiki_platform_id = PLATFORM['tiki']
        categories_db = read_data_from_postgres(spark, 'dim_category')
        categories_db = categories_db.filter(col('platform_id') == lit(tiki_platform_id))

        diff_categories_df = spark.createDataFrame([], schema=new_categories_df.schema)

        for level in range(new_categories_df.select("level").distinct().count()):
            # Step 1: Read existing data from the dim_category table in the database
            categories_db_level = categories_db.filter(col('level') == lit(level))
            new_categories_level = new_categories_df.filter(col('level') == lit(level))

            # Step 2: Identify new records
            new_records_df = new_categories_level.alias('new').join(categories_db_level.alias('existing'),
                                                                    on=['category_code', 'platform_id'],
                                                                    how='left_anti')
            if new_records_df.count() > 0:
                new_records_df = new_records_df.select("new.*")
                diff_categories_df.union(new_records_df)

        return diff_categories_df

    def write_data_to_postgres(self, spark: SparkSession, diff_categories_df: DataFrame, table_name: str) -> None:
        tiki_platform_id = PLATFORM['tiki']
        categories_db = read_data_from_postgres(spark, 'dim_category')
        categories_db = categories_db.filter(col('platform_id') == lit(tiki_platform_id))

        distinct_levels = diff_categories_df.select("level").distinct().collect()
        for row in distinct_levels:
            level = row["level"]
            diff_categories_level = diff_categories_df.filter(col('level') == lit(level))

            if level > 0:
                categories_db_level = categories_db.filter(col('level') == lit(level - 1))
                diff_categories_level = diff_categories_level.alias("new").join(
                    categories_db_level.select(
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
                diff_categories_level = diff_categories_level.withColumn("parent_id", lit(None))

            diff_categories_level = diff_categories_level.select("category_code",
                                                                 "platform_id",
                                                                 "category_name",
                                                                 "category_url",
                                                                 col("parent_id").cast('int').alias('parent_id'),
                                                                 "level")

            super().write_data_to_postgres(
                spark,
                diff_categories_level,
                table_name
            )


class BrandIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, new_data: DataFrame) -> Any:
        # Step 1: Read existing data from the dim_brand table
        brands_db = read_data_from_postgres(spark, 'dim_brand')
        platform_id = new_data.select('platform_id').first()['platform_id']
        brands_db = brands_db.filter((col('platform_id') == platform_id))

        # Step 2: Identify new records
        new_records_df = new_data.join(brands_db,
                                       on=['brand_code', 'platform_id'],
                                       how='left_anti')

        return new_records_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class SellerIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, new_data: DataFrame) -> Any:
        # Step 1: Read existing data from the dim_brand table
        sellers_db = read_data_from_postgres(spark, 'dim_seller')
        platform_id = new_data.select('platform_id').first()['platform_id']
        sellers_db = sellers_db.filter((col('platform_id') == platform_id))

        # Step 2: Identify new records
        new_records_df = new_data.join(sellers_db,
                                       on=['seller_code', 'platform_id'],
                                       how='left_anti')

        return new_records_df

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


class ProductIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, new_data: DataFrame) -> Any:
        # Step 1: Read existing data from the dim_product table in the database
        products_db = read_data_from_postgres(spark, 'dim_product')
        platform_id = new_data.select('platform_id').first()['platform_id']
        products_db = products_db.filter((col('platform_id') == platform_id) & col('flag'))

        # Step 2: Identify new records
        new_records_df = new_data.alias('new').join(products_db.alias('existing'),
                                                    on=['product_code', 'platform_id', 'seller_id'],
                                                    how='left_anti')

        # Step 3: Identify updated records
        matched_records_df = new_data.alias('new').join(products_db.alias('existing'),
                                                        on=['product_code', 'platform_id', 'seller_id'],
                                                        how='inner')

        updated_records_df = matched_records_df.filter(
            (col('new.product_name') != col('existing.product_name')) |
            (col('new.product_url') != col('existing.product_url')) |
            (col('new.price') != col('existing.price')) |
            (col('new.original_price') != col('existing.original_price')) |
            (col('new.discount_rate') != col('existing.discount_rate')) |
            (col('new.rating_score') != col('existing.rating_score')) |
            (col('new.review_count') != col('existing.review_count')) |
            (col('new.sold') != col('existing.sold'))
        ).select('new.*')

        new_df_copy = spark.createDataFrame(new_records_df.rdd, new_records_df.schema)
        updated_df_copy = spark.createDataFrame(updated_records_df.rdd, updated_records_df.schema)

        return new_df_copy, updated_df_copy

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)

    def update_data_to_postgres(self, updated_df: DataFrame, table_name: str):
        batch_size = 100
        updated_keys_df = updated_df.select('product_code', 'platform_id', 'seller_id').distinct()

        # Collect the keys to update
        updated_keys = updated_keys_df.collect()

        # Convert to a format suitable for SQL IN clause
        keys_to_update = [(row['product_code'], row['platform_id'], row['seller_id']) for row in
                          updated_keys]

        if keys_to_update:
            for i in range(0, len(keys_to_update), batch_size):
                batch = keys_to_update[i:i + batch_size]
                # Build the SQL query for batch update
                # todo: change the expired date if needed
                query = """
                        WITH latest_records AS (
                            SELECT product_code, platform_id, seller_id, effective_date
                            FROM (
                                SELECT product_code, platform_id, seller_id, effective_date,
                                       ROW_NUMBER() OVER (PARTITION BY product_code, platform_id, seller_id ORDER BY effective_date DESC) as rn
                                FROM ecommerce.dim_product
                            ) sub
                            WHERE rn = 1
                        )
                        UPDATE ecommerce.dim_product p
                        SET expired_date = '2024-07-21', -- '2024-07-21'
                            flag = FALSE
                        FROM latest_records l
                        WHERE p.product_code = l.product_code
                          AND p.platform_id = l.platform_id
                          AND p.seller_id = l.seller_id
                          AND p.effective_date = l.effective_date
                          AND (p.product_code, p.platform_id, p.seller_id) IN ({})
                """.format(','.join(["('{}', {}, {})".format(*key) for key in batch]))

                self.logger.info(f"Updating batch {i} in {table_name}")
                update_existing_records(query)


class FactSaleIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, new_fact_sale_df: DataFrame) -> DataFrame:
        fact_sale_db = read_data_from_postgres(spark, 'fact_sales')
        platform_id = new_fact_sale_df.select('platform_id').first()['platform_id']
        fact_sale_db = fact_sale_db.filter((col('platform_id') == platform_id))

        diff_fact_sale_df = new_fact_sale_df.alias('new').join(fact_sale_db.alias('existing'),
                                                               on=['category_id', 'brand_id', 'seller_id',
                                                                   'product_id', 'location_id', 'platform_id'],
                                                               how='left_anti')

        # Group fact_sale_db by (product_code, seller_id, platform_id)
        window_spec = Window.partitionBy('product_code', 'seller_id', 'platform_id').orderBy(col('date_id').desc())
        fact_sale_db_filtered = fact_sale_db.withColumn('row_number', row_number().over(window_spec)) \
            .filter(col('row_number') == lit(1)) \
            .drop('row_number')

        # Join diff_fact_sale_df and fact_sale_db_filtered
        updated_df = diff_fact_sale_df.alias('new').join(
            fact_sale_db_filtered.alias('existing'),
            on=['product_code', 'seller_id', 'platform_id'],
            how='left'
        )

        # Calculate the updated total_sales_amount based on the condition
        updated_df = updated_df.withColumn(
            'updated_total_sales_amount',
            when(col('existing.sale_id').isNull(), col('new.total_sales_amount')).otherwise(
                col('existing.total_sales_amount') +
                (col('new.units_sold') - col('existing.units_sold')) *
                (col('new.total_sales_amount') / col('new.units_sold'))
            )
        )

        # Cast all null values in updated_total_sales_amount to 0.00
        updated_df = updated_df.withColumn(
            'updated_total_sales_amount',
            coalesce(col('updated_total_sales_amount'), lit(0.00))
        )
        # Select relevant columns and replace total_sales_amount with the updated value
        updated_df = updated_df.select(
            col('new.date_id'),
            col('new.platform_id'),
            col('new.category_id'),
            col('new.brand_id'),
            col('new.seller_id'),
            col('new.product_id'),
            col('new.product_code'),
            col('new.location_id'),
            col('new.units_sold'),
            col('updated_total_sales_amount').alias('total_sales_amount')
        )

        updated_df_copy = spark.createDataFrame(updated_df.rdd, updated_df.schema)
        return updated_df_copy

    def write_data_to_postgres(self, spark: SparkSession, insert_df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, insert_df, table_name)


class FactPeriodicIncrementalETL(BaseETL):
    def process_data(self, spark: SparkSession, platform_id: int) -> Any:
        # Load the fact_sales and dim_date data
        fact_sales_df = read_data_from_postgres(spark, "fact_sales")
        fact_sales_df = fact_sales_df.filter(F.col("platform_id") == F.lit(platform_id))
        # Step 1: Get the maximum date_id of the whole table
        max_date_id_of_fact_sale = fact_sales_df.agg(F.max("date_id").alias("max_date_id")).collect()[0]["max_date_id"]

        # Step 2: Get the maximum date_id for each group
        window_spec = Window.partitionBy("product_code", "seller_id", "platform_id")
        fact_sales_df_with_max_date = fact_sales_df.withColumn("max_date_id_of_group",
                                                               F.max("date_id").over(window_spec))

        # Step 3: Filter records where the group's max date_id is equal to the table's max date_id
        fact_sales_df_filtered = fact_sales_df_with_max_date.filter(
            F.col("max_date_id_of_group") == max_date_id_of_fact_sale)

        # Drop the extra column used for filtering
        fact_sales_df_filtered = fact_sales_df_filtered.drop("max_date_id_of_group")

        dim_date_df = read_data_from_postgres(spark, "dim_date")

        # Register DataFrames as temporary views
        fact_sales_df_filtered.createOrReplaceTempView("fact_sales")
        dim_date_df.createOrReplaceTempView("dim_date")

        full_query = """
                    WITH ranked_sales AS (SELECT fs.sale_id,
                                                 fs.date_id,
                                                 fs.platform_id,
                                                 fs.category_id,
                                                 fs.brand_id,
                                                 fs.seller_id,
                                                 fs.product_id,
                                                 fs.location_id,
                                                 fs.units_sold,
                                                 fs.total_sales_amount,
                                                 fs.product_code,
                                                 d.date,
                                                 ROW_NUMBER()
                                                 OVER (PARTITION BY fs.product_code, fs.seller_id, fs.platform_id ORDER BY fs.date_id DESC) as rn
                                          FROM fact_sales fs
                                                   JOIN
                                               dim_date d ON fs.date_id = d.date_id),
                         latest_sales AS (SELECT rs1.product_code,
                                                 rs1.seller_id,
                                                 rs1.platform_id,
                                                 rs1.category_id,
                                                 rs1.product_id,
                                                 rs1.brand_id,
                                                 rs1.location_id,
                                                 rs1.date_id            as latest_date_id,
                                                 rs1.units_sold         as latest_units_sold,
                                                 rs1.total_sales_amount as latest_total_sales_amount,
                                                 rs1.date               as latest_date
                                          FROM ranked_sales rs1
                                          WHERE rs1.rn = 1),
                         growth_calculation AS (SELECT ls.latest_date_id, 
                                                       ls.platform_id,
                                                       ls.category_id,
                                                       ls.seller_id,
                                                       ls.product_id,
                                                       ls.product_code,
                                                       ls.brand_id,
                                                       ls.location_id,
                                                       ls.latest_units_sold,
                                                       ls.latest_total_sales_amount,
                                                       ls.latest_date,
                                                       rs.units_sold,
                                                       rs.total_sales_amount,
                                                       rs.date,
                                                       (ls.latest_units_sold - rs.units_sold)                 as diff_units_sold,
                                                       (ls.latest_total_sales_amount - rs.total_sales_amount) as diff_total_sales_amount,
                                                       DATEDIFF(ls.latest_date, rs.date)                      as diff_date
                                                FROM ranked_sales rs
                                                         JOIN
                                                     latest_sales ls
                                                     ON rs.product_code = ls.product_code AND rs.seller_id = ls.seller_id AND
                                                        rs.platform_id = ls.platform_id
                                                WHERE rs.rn > 1)
                    SELECT latest_date_id          as date_id,
                           platform_id,
                           category_id,
                           seller_id,
                           product_id,
                           product_code,
                           brand_id,
                           location_id,
                           diff_units_sold         as units_sold,
                           diff_total_sales_amount as total_sales_amount,
                           CASE
                               WHEN diff_date <= 7 THEN '7-DAY'
                               WHEN diff_date > 7 AND diff_date <= 14 THEN '14-DAY'
                               WHEN diff_date > 14 AND diff_date <= 21 THEN '21-DAY'
                               WHEN diff_date > 21 AND diff_date <= 30 THEN '30-DAY'
                               ELSE '60-DAY'
                               END                 as report_period
                    FROM growth_calculation
                    WHERE diff_units_sold > 0 AND diff_total_sales_amount > 0
                    ORDER BY product_code, seller_id, platform_id;  
        """

        # Execute the full query
        fact_periodic_sales_df = spark.sql(full_query)

        # Check diff fact periodic sales
        fact_periodic_sales_db = read_data_from_postgres(spark, 'fact_periodic_sales')
        fact_periodic_sales_db = fact_periodic_sales_db.filter((col('platform_id') == lit(platform_id)))

        diff_fact_periodic_sales_df = fact_periodic_sales_df.alias('new').join(fact_periodic_sales_db.alias('existing'),
                                                                               on=['category_id', 'brand_id',
                                                                                   'seller_id',
                                                                                   'product_id', 'location_id',
                                                                                   'platform_id'],
                                                                               how='left_anti')

        diff_fact_periodic_sales_df_copy = spark.createDataFrame(diff_fact_periodic_sales_df.rdd,
                                                                 diff_fact_periodic_sales_df.schema)
        return diff_fact_periodic_sales_df_copy

    def write_data_to_postgres(self, spark: SparkSession, df: DataFrame, table_name: str):
        super().write_data_to_postgres(spark, df, table_name)


def runner():
    spark = SparkSessionSingleton.get_instance()
    tiki_data_path = "./data/tiki/*.json"  # todo: change if required
    tiki_data = spark.read.json(tiki_data_path)

    # dim_category incremental load
    new_categories_df = InitialCategory().process_data(spark, tiki_data)
    diff_categories_df = CategoryIncrementalETL().process_data(spark, new_categories_df)

    diff_categories_df.persist()
    if diff_categories_df.count() > 0:
        CategoryIncrementalETL().write_data_to_postgres(spark, diff_categories_df, 'dim_category')
    diff_categories_df.unpersist()

    # dim_brand incremental load
    new_brands_df = InitialBrand().process_data(spark, tiki_data)
    diff_brands_df = BrandIncrementalETL().process_data(spark, new_brands_df)

    diff_brands_df.persist()
    if diff_brands_df.count() > 0:
        BrandIncrementalETL().write_data_to_postgres(spark, diff_brands_df, 'dim_brand')
    diff_brands_df.unpersist()

    # dim_seller incremental load
    new_sellers_df = InitialSeller().process_data(spark, tiki_data)
    diff_sellers_df = SellerIncrementalETL().process_data(spark, new_sellers_df)

    diff_sellers_df.persist()
    if diff_sellers_df.count() > 0:
        SellerIncrementalETL().write_data_to_postgres(spark, diff_sellers_df, 'dim_seller')
    diff_sellers_df.unpersist()

    # dim_product incremental load
    new_products_df = InitialProduct().process_data(spark, tiki_data)
    new_df, updated_df = ProductIncrementalETL().process_data(spark, new_products_df)

    if updated_df.count() > 0:
        ProductIncrementalETL().update_data_to_postgres(updated_df, 'dim_product')

    insertion_df = new_df.union(updated_df)
    if insertion_df.count() > 0:
        ProductIncrementalETL().write_data_to_postgres(spark, insertion_df, 'dim_product')

    # fact_sale init load for new records
    new_fact_sale_df = InitialFactSale().process_data(spark, tiki_data)
    diff_fact_sale = FactSaleIncrementalETL().process_data(spark, new_fact_sale_df)
    diff_fact_sale.persist()
    if diff_fact_sale.count() > 0:
        FactSaleIncrementalETL().write_data_to_postgres(spark, diff_fact_sale, 'fact_sales')
    diff_fact_sale.unpersist()

    # fact_periodic_sales incremental load
    fact_periodic = FactPeriodicIncrementalETL()
    fact_periodic_df = fact_periodic.process_data(spark, PLATFORM['tiki'])
    if fact_periodic_df.count() > 0:
        fact_periodic.write_data_to_postgres(spark, fact_periodic_df, 'fact_periodic_sales')


if __name__ == "__main__":
    runner()
