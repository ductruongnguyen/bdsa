import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType
from tiki_incremental_load import BrandIncrementalETL, SellerIncrementalETL
from decimal import Decimal


class TestBrandIncrementalETL(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("unit_tests") \
            .getOrCreate()

        self.brand_etl = BrandIncrementalETL()

        self.sample_new_data = [
            (1, 12, "BrandA"),
            (2, 12, "BrandB"),
            (3, 12, "BrandC")
        ]

        self.sample_existing_data = [
            (1, 12, "BrandA"),
            (2, 12, "BrandB")
        ]

        schema = StructType([
            StructField("brand_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("brand_name", StringType(), nullable=False)
        ])

        self.new_brands_df = self.spark.createDataFrame(self.sample_new_data, schema=schema)
        self.existing_brands_df = self.spark.createDataFrame(self.sample_existing_data, schema=schema)

    def tearDown(self):
        self.spark.stop()

    @patch('tiki_incremental_load.read_data_from_postgres')
    def test_process_data(self, mock_read_data_from_postgres):
        mock_read_data_from_postgres.return_value = self.existing_brands_df

        result_df = self.brand_etl.process_data(self.spark, self.new_brands_df)

        expected_schema = StructType([
            StructField("brand_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("brand_name", StringType(), nullable=False)
        ])

        expected_data = [
            (3, 12, "BrandC")
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertTrue(result_df.schema == expected_df.schema)
        self.assertTrue(result_df.collect() == expected_df.collect())


class TestSellerIncrementalETL(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("unit_tests") \
            .getOrCreate()

        self.seller_etl = SellerIncrementalETL()

        self.sample_new_data = [
            (1, 12, "SellerA", "http://example.com/sellerA"),
            (2, 12, "SellerB", "http://example.com/sellerB"),
            (3, 12, "SellerC", "http://example.com/sellerC")
        ]

        self.sample_existing_data = [
            (1, 12, "SellerA", "http://example.com/sellerA"),
            (2, 12, "SellerB", "http://example.com/sellerB")
        ]

        schema = StructType([
            StructField("seller_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("seller_name", StringType(), nullable=False),
            StructField("seller_url", StringType(), nullable=True)
        ])

        self.new_sellers_df = self.spark.createDataFrame(self.sample_new_data, schema=schema)
        self.existing_sellers_df = self.spark.createDataFrame(self.sample_existing_data, schema=schema)

    def tearDown(self):
        self.spark.stop()

    @patch('tiki_incremental_load.read_data_from_postgres')
    def test_process_data(self, mock_read_data_from_postgres):
        mock_read_data_from_postgres.return_value = self.existing_sellers_df

        result_df = self.seller_etl.process_data(self.spark, self.new_sellers_df)

        expected_schema = StructType([
            StructField("seller_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("seller_name", StringType(), nullable=False),
            StructField("seller_url", StringType(), nullable=True)
        ])

        expected_data = [
            (3, 12, "SellerC", "http://example.com/sellerC")
        ]

        expected_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertTrue(result_df.schema == expected_df.schema)
        self.assertTrue(result_df.collect() == expected_df.collect())


if __name__ == '__main__':
    unittest.main()
