import unittest
import pyspark.sql
from unittest.mock import patch, MagicMock, Mock
from general_initial_load import insert_dim_date, insert_dim_seller, insert_dim_brand, insert_dim_category, \
    insert_dim_location
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession


class TestInsertDimDate(unittest.TestCase):
    def setUp(self):
        self.spark = pyspark.sql.SparkSession.builder.getOrCreate()

    @patch("general_initial_load.read_data_from_postgres")
    @patch("general_initial_load.insert_spark_df_to_postgres")
    def test_insert_dim_date_when_date_not_in_db(self, mock_insert, mock_read):
        mock_read.return_value = self.spark.createDataFrame(pd.DataFrame({'date': ['2025-01-01']}))
        insert_dim_date(self.spark)
        mock_insert.assert_called_once()

    @patch("general_initial_load.read_data_from_postgres")
    @patch("general_initial_load.insert_spark_df_to_postgres")
    def test_insert_dim_date_when_date_in_db(self, mock_insert, mock_read):
        mock_read.return_value = self.spark.createDataFrame(pd.DataFrame({'date': [datetime.now().date()]}))
        insert_dim_date(self.spark)
        mock_insert.assert_not_called()

    @patch('general_initial_load.get_platform_pd')
    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_insert_dim_seller_exists(self, mock_insert_spark_df_to_postgres, mock_check_exist_records,
                                      mock_get_platform_pd):
        mock_get_platform_pd.return_value = pd.DataFrame({'platform_id': [1]})
        mock_check_exist_records.return_value = True

        mock_spark = Mock(spec=pyspark.sql.SparkSession)

        insert_dim_seller(mock_spark)

        mock_insert_spark_df_to_postgres.assert_not_called()

    @patch('general_initial_load.get_platform_pd')
    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_insert_dim_seller_not_exists(self, mock_insert_spark_df_to_postgres, mock_check_exist_records,
                                          mock_get_platform_pd):
        mock_get_platform_pd.return_value = pd.DataFrame({'platform_id': [1]})
        mock_check_exist_records.return_value = False

        mock_spark = Mock(spec=pyspark.sql.SparkSession)

        insert_dim_seller(mock_spark)

        mock_insert_spark_df_to_postgres.assert_called_once()

    @patch('general_initial_load.get_platform_pd')
    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_insert_dim_brand_existing_record(self, mocked_insert, mocked_check, mocked_get):
        mocked_get.return_value = pd.DataFrame({'platform_id': [1]})
        mocked_check.return_value = True

        insert_dim_brand(self.spark)

        mocked_insert.assert_not_called()

    @patch('general_initial_load.get_platform_pd')
    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_insert_dim_brand_non_existing_record(self, mocked_insert, mocked_check, mocked_get):
        mocked_get.return_value = pd.DataFrame({'platform_id': [1]})
        mocked_check.return_value = False

        insert_dim_brand(self.spark)

        mocked_insert.called_once()

    @patch("general_initial_load.get_platform_pd")
    @patch("general_initial_load.check_exist_records")
    @patch("general_initial_load.insert_spark_df_to_postgres")
    def test_insert_dim_category_new_record(self, mock_insert_spark_df_to_postgres, mock_check_exist_records,
                                            mock_get_platform_pd):
        mock_get_platform_pd.return_value = pd.DataFrame({'platform_id': [1]})
        mock_check_exist_records.return_value = False
        mock_spark = Mock(spec=pyspark.sql.SparkSession)

        # Act
        insert_dim_category(mock_spark)

        # Assert
        mock_get_platform_pd.assert_called_once_with(mock_spark)
        mock_check_exist_records.assert_called_once()
        mock_insert_spark_df_to_postgres.assert_called_once()

    @patch("general_initial_load.get_platform_pd")
    @patch("general_initial_load.check_exist_records")
    def test_insert_dim_category_existing_record(self, mock_check_exist_records, mock_get_platform_pd):
        mock_get_platform_pd.return_value = pd.DataFrame({'platform_id': [1]})
        mock_check_exist_records.return_value = True
        mock_spark = Mock(spec=pyspark.sql.SparkSession)

        # Act
        insert_dim_category(mock_spark)

        # Assert
        mock_get_platform_pd.assert_called_once_with(mock_spark)
        mock_check_exist_records.assert_called_once()

    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_location_exists(self, mock_insert, mock_exist_records):
        mock_exist_records.return_value = True

        insert_dim_location(self.spark)

        mock_exist_records.assert_called_once()
        self.assertFalse(mock_insert.called)

    @patch('general_initial_load.check_exist_records')
    @patch('general_initial_load.insert_spark_df_to_postgres')
    def test_location_does_not_exist(self, mock_insert, mock_exist_records):
        mock_exist_records.return_value = False

        insert_dim_location(self.spark)

        mock_exist_records.assert_called_once()
        mock_insert.assert_called_once()


if __name__ == '__main__':
    unittest.main()
