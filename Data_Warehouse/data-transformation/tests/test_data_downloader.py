import unittest
from unittest.mock import Mock, patch
from data_downloader import download_lazada_data


class TestDownloadLazadaData(unittest.TestCase):
    @patch('data_downloader.prepare_local_directory')
    @patch('data_downloader.get_most_recent_file_s3')
    @patch('data_downloader.extract_file_date_time')
    @patch('data_downloader.get_list_most_recent_files_s3')
    @patch('data_downloader.download_files_from_s3')
    def test_download_lazada_data_success(self, mock_download, mock_get_list, mock_extract, mock_get_recent, mock_prepare):
        mock_prepare.return_value = None
        mock_get_recent.return_value = {'Key': 'test_key'}
        mock_extract.return_value = '2024-06-12'
        mock_get_list.return_value = [{'Key': 'test_key'}]
        mock_download.return_value = None

        download_lazada_data()

        mock_prepare.assert_called_once_with('./data/lazada/1')
        mock_get_recent.assert_called_once_with('lazada/raw/product/', 'products')
        mock_extract.assert_called_once_with('test_key')
        mock_get_list.assert_called_once_with('lazada/raw/product/', 'products', '2024-06-12')
        mock_download.assert_called_once_with([{'Key': 'test_key'}], './data/lazada/1')

    @patch('data_downloader.prepare_local_directory')
    @patch('data_downloader.get_most_recent_file_s3')
    @patch('data_downloader.extract_file_date_time')
    @patch('data_downloader.get_list_most_recent_files_s3')
    @patch('data_downloader.download_files_from_s3')
    @patch('builtins.print')
    def test_download_lazada_data_no_recent_files(self, mock_print, mock_download, mock_get_list, mock_extract, mock_get_recent, mock_prepare):
        mock_prepare.return_value = None
        mock_get_recent.return_value = {'Key': 'test_key'}
        mock_extract.return_value = '2024-06-12'
        mock_get_list.return_value = []
        mock_download.return_value = None

        download_lazada_data()

        mock_prepare.assert_called_once_with('./data/lazada/1')
        mock_get_recent.assert_called_once_with('lazada/raw/product/', 'products')
        mock_extract.assert_called_once_with('test_key')
        mock_get_list.assert_called_once_with('lazada/raw/product/', 'products', '2024-06-12')
        mock_download.assert_not_called()
        mock_print.assert_called_once_with('No recent files found.')


if __name__ == '__main__':
    unittest.main()
