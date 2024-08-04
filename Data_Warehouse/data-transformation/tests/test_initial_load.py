import json
import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from tiki_initial_load import flatten_category, extract_category_df, CategoryETL, BrandETL, SellerETL


class TestCategoryETL(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("unit_tests") \
            .getOrCreate()

        self.category_etl = CategoryETL()

        self.sample_json_data = [
            {
                "category_id": "1",
                "category_name": "Electronics",
                "category_urlKey": "electronics",
                "children": [
                    {
                        "category_id": "2",
                        "category_name": "Mobile Phones",
                        "category_urlKey": "mobile-phones"
                    }
                ]
            }
        ]

        self.flattened_categories = [
            {
                "category_code": 1,
                "platform_id": 12,
                "category_name": "Electronics",
                "category_url": "electronics",
                "parent_code": -1,
                "level": 0
            },
            {
                "category_code": 2,
                "platform_id": 12,
                "category_name": "Mobile Phones",
                "category_url": "mobile-phones",
                "parent_code": 1,
                "level": 1
            }
        ]

        schema = StructType([
            StructField("category_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("category_name", StringType(), nullable=False),
            StructField("category_url", StringType(), nullable=True),
            StructField("parent_code", LongType(), nullable=True),
            StructField("level", IntegerType(), nullable=False)
        ])

        sample_data = [
            (1, 12, "Electronics", "electronics", -1, 0),
            (2, 12, "Mobile Phones", "mobile-phones", 1, 1)
        ]

        self.categories_df = self.spark.createDataFrame(sample_data, schema=schema)

    def tearDown(self):
        self.spark.stop()

    def test_flatten_category(self):
        result = flatten_category(self.sample_json_data, platform_id=12)
        self.assertEqual(result, self.flattened_categories)

    def test_extract_category_df(self):
        result_df = extract_category_df(self.spark, self.sample_json_data)
        self.assertTrue(result_df.schema == self.categories_df.schema)
        self.assertTrue(result_df.collect() == self.categories_df.collect())

    @patch('tiki_initial_load.get_most_recent_file_s3')
    @patch('tiki_initial_load.S3_CLIENT')
    @patch('tiki_initial_load.extract_category_df')
    def test_process_data(self, mock_extract_category_df, mock_s3_client, mock_get_most_recent_file_s3):
        mock_get_most_recent_file_s3.return_value = {'Key': 'dummy_key'}
        mock_s3_client.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=json.dumps(self.sample_json_data).encode('utf-8')))
        }
        mock_extract_category_df.return_value = self.categories_df

        result_df = self.category_etl.process_data(self.spark, None)
        self.assertTrue(result_df.schema == self.categories_df.schema)
        self.assertTrue(result_df.collect() == self.categories_df.collect())

    @patch('tiki_initial_load.insert_spark_df_to_postgres')
    @patch('tiki_initial_load.read_data_from_postgres')
    def test_write_data_to_postgres(self, mock_read_data_from_postgres, mock_insert_spark_df_to_postgres):
        schema_inserted = StructType([
            StructField("category_id", IntegerType(), nullable=False),
            StructField("category_code", LongType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("category_name", StringType(), nullable=False),
            StructField("category_url", StringType(), nullable=True),
            StructField("parent_id", IntegerType(), nullable=True),
            StructField("level", IntegerType(), nullable=False)
        ])

        # Mock the return value of read_data_from_postgres with an empty DataFrame with the same schema
        empty_df = self.spark.createDataFrame([], schema=schema_inserted)
        mock_read_data_from_postgres.return_value = empty_df

        self.category_etl.write_data_to_postgres(self.spark, self.categories_df, 'test_table')

        self.assertTrue(mock_insert_spark_df_to_postgres.called)
        self.assertEqual(mock_insert_spark_df_to_postgres.call_count, 2)


class TestBrandETL(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("unit_tests") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
            .getOrCreate()

        self.brand_etl = BrandETL()

        self.sample_json_data = [
            {"data": {"brand": {"id": 1, "name": "BrandA"}}},
            {"data": {"brand": {"id": 2, "name": "BrandB"}}},
            {"data": {"brand": {"id": 3, "name": "BrandC"}}}
        ]

        schema = StructType([
            StructField("brand_code", LongType(), nullable=False),
            StructField("brand_name", StringType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False)
        ])

        sample_data = [
            (1, "BrandA", 12),
            (2, "BrandB", 12),
            (3, "BrandC", 12)
        ]

        self.brands_df = self.spark.createDataFrame(sample_data, schema=schema)

    def tearDown(self):
        self.spark.stop()

    def test_process_data(self):
        schema = StructType([
            StructField("data", StructType([
                StructField("brand", StructType([
                    StructField("id", LongType(), nullable=True),
                    StructField("name", StringType(), nullable=True)
                ]), nullable=True)
            ]), nullable=True)
        ])

        input_df = self.spark.createDataFrame(self.sample_json_data, schema=schema)
        result_df = self.brand_etl.process_data(self.spark, input_df)

        self.assertTrue(result_df.collect() == self.brands_df.collect())


class TestSellerETL(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("unit_tests") \
            .getOrCreate()

        self.seller_etl = SellerETL()

        self.sample_json_data = [
            {"data": {"current_seller": {"id": 1, "name": "SellerA", "link": "http://example.com/sellerA"}}},
            {"data": {"current_seller": {"id": 2, "name": "SellerB", "link": "http://example.com/sellerB"}}},
            {"data": {"current_seller": {"id": 3, "name": "SellerC", "link": "http://example.com/sellerC"}}}
        ]

        schema = StructType([
            StructField("seller_code", LongType(), nullable=False),
            StructField("seller_name", StringType(), nullable=False),
            StructField("platform_id", IntegerType(), nullable=False),
            StructField("seller_url", StringType(), nullable=True)
        ])

        sample_data = [
            (1, "SellerA", 12, "http://example.com/sellerA"),
            (2, "SellerB", 12, "http://example.com/sellerB"),
            (3, "SellerC", 12, "http://example.com/sellerC")
        ]

        self.sellers_df = self.spark.createDataFrame(sample_data, schema=schema)

    def tearDown(self):
        self.spark.stop()

    def test_process_data(self):
        schema = StructType([
            StructField("data", StructType([
                StructField("current_seller", StructType([
                    StructField("id", LongType(), nullable=True),
                    StructField("name", StringType(), nullable=True),
                    StructField("link", StringType(), nullable=True)
                ]), nullable=True)
            ]), nullable=True)
        ])

        input_df = self.spark.createDataFrame(self.sample_json_data, schema=schema)
        result_df = self.seller_etl.process_data(self.spark, input_df)

        self.assertTrue(result_df.collect() == self.sellers_df.collect())


if __name__ == "__main__":
    unittest.main()
