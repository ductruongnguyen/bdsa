import json
import unittest
from unittest.mock import patch, MagicMock

from projects.tiki.tiki.items import ProductList
from projects.tiki.tiki.spiders.prd_list_crawler import UrlsCrawlerSpider


class TestUrlsCrawlerSpider(unittest.TestCase):

    def setUp(self):
        self.spider = UrlsCrawlerSpider()

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_start_requests_success(self, mock_send_error_email):
        categories = [
            {"category_id": "1", "category_urlKey": "key1"},
            {"category_id": "2", "category_urlKey": "key2"}
        ]
        with patch.object(self.spider, 'load_categories_from_s3', return_value=categories):
            with patch.object(self.spider, 'extract_no_children', return_value=categories):
                requests = list(self.spider.start_requests())
                self.assertEqual(len(requests), 2)
                self.assertEqual(requests[0].meta['cat_id'], '1')
                self.assertEqual(requests[0].meta['key'], 'key1')
                self.assertEqual(requests[1].meta['cat_id'], '2')
                self.assertEqual(requests[1].meta['key'], 'key2')
        mock_send_error_email.assert_not_called()

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_start_requests_failed(self, mock_send_error_email):
        categories = [
            {"category_id": "1", "category_invalid_field": "key1"}
        ]
        with patch.object(self.spider, 'load_categories_from_s3', return_value=categories):
            with patch.object(self.spider, 'extract_no_children', return_value=categories):
                requests = list(self.spider.start_requests())
                self.assertEqual(requests, [])

        mock_send_error_email.assert_called_once()

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_parse_initial_page_success(self, mock_send_error_email):
        response = MagicMock()
        response.meta = {'cat_id': '1', 'key': 'key1'}
        response.body = json.dumps({"paging": {"last_page": 3}}).encode('utf-8')
        requests = list(self.spider.parse_initial_page(response))
        self.assertEqual(len(requests), 3)
        self.assertEqual(requests[0].meta['url'],
                         'https://tiki.vn/api/v2/products?category=1&urlKey=key1&page=1&limit=50')
        mock_send_error_email.assert_not_called()

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_parse_initial_page_failed(self, mock_send_error_email):
        response = MagicMock()
        response.meta = {'cat_id': '1', 'key': 'key1'}
        response.body = json.dumps({"paging_invalid": {"last_page": 3}}).encode('utf-8')
        requests = list(self.spider.parse_initial_page(response))
        self.assertEqual(requests, [])
        mock_send_error_email.assert_called_once()

    @patch('boto3.client')
    def test_load_categories_from_s3(self, mock_boto3_client):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'cat-1', 'LastModified': '2024-07-10T12:00:00Z'},
                {'Key': 'cat-2', 'LastModified': '2024-07-11T12:00:00Z'}
            ]
        }
        mock_s3.get_object.return_value = {
            "Body": MagicMock(
                read=lambda: json.dumps([{"category_id": "1", "category_urlKey": "key1"}]).encode('utf-8'))
        }

        categories = self.spider.load_categories_from_s3()
        self.assertIsInstance(categories, list)
        self.assertEqual(categories[0]['category_id'], '1')
        self.assertEqual(categories[0]['category_urlKey'], 'key1')

    def test_extract_no_children(self):
        categories = [
            {"category_id": "1", "category_name": "cat1", "category_urlKey": "key1"},
            {"category_id": "2", "category_name": "cat2", "category_urlKey": "key2", "children": [
                {"category_id": "3", "category_name": "cat3", "category_urlKey": "key3"}
            ]}
        ]
        result = self.spider.extract_no_children(categories)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['category_id'], '1')
        self.assertEqual(result[1]['category_id'], '3')

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_parse_success(self, mock_send_error_email):
        response = MagicMock()
        response.meta = {'url': 'some_url'}
        response.body = json.dumps(
            {"data": [{"id": "1", "seller_product_id": "10"}, {"id": "2", "seller_product_id": "20"}]}).encode('utf-8')
        items = list(self.spider.parse(response))
        self.assertEqual(len(items), 2)
        self.assertIsInstance(items[0], ProductList)
        self.assertEqual(items[0]['product_id'], '1')
        self.assertEqual(items[0]['seller_product_id'], '10')
        mock_send_error_email.assert_not_called()

    @patch('projects.tiki.tiki.spiders.prd_list_crawler.Mailer.send_error_email')
    def test_parse_error(self, mock_send_error_email):
        response = MagicMock()
        response.meta = {'url': 'some_url'}
        response.body = 'invalid_json'
        list(self.spider.parse(response))
        mock_send_error_email.assert_called_once()


if __name__ == '__main__':
    unittest.main()
