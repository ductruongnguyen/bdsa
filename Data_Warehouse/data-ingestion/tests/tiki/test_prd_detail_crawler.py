import json
import unittest
from unittest.mock import patch, MagicMock

from projects.tiki.tiki.items import ProductDetail
from projects.tiki.tiki.spiders.prd_detail_crawler import PrdDetailCrawlerSpider


class TestPrdDetailCrawlerSpider(unittest.TestCase):

    def setUp(self):
        self.spider = PrdDetailCrawlerSpider()

    @patch('projects.tiki.tiki.spiders.prd_detail_crawler.Mailer.send_error_email')
    def test_start_requests(self, mock_send_error_email):
        urls = ['https://tiki.vn/api/v2/products/1?platform=web&spid=10',
                'https://tiki.vn/api/v2/products/2?platform=web&spid=20']
        with patch.object(self.spider, 'urls_extractor', return_value=urls):
            requests = list(self.spider.start_requests())
            self.assertEqual(len(requests), 2)
            self.assertEqual(requests[0].url, urls[0])
            self.assertEqual(requests[1].url, urls[1])
        mock_send_error_email.assert_not_called()

    def test_urls_extractor(self):
        products = [{'product_id': '1', 'seller_product_id': '10'}, {'product_id': '2', 'seller_product_id': '20'}]
        with patch.object(self.spider, 'load_product_list_from_s3', return_value=products):
            urls = self.spider.urls_extractor()
            self.assertEqual(len(urls), 2)
            self.assertIn('https://tiki.vn/api/v2/products/1?platform=web&spid=10', urls)
            self.assertIn('https://tiki.vn/api/v2/products/2?platform=web&spid=20', urls)

    @patch('boto3.client')
    def test_load_product_list_from_s3(self, mock_boto3_client):
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'list-1', 'LastModified': '2024-07-10T12:00:00Z'},
                {'Key': 'list-2', 'LastModified': '2024-07-11T12:00:00Z'}
            ]
        }
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: json.dumps([{"product_id": "1", "seller_product_id": "10"}]).encode('utf-8'))
        }

        products = self.spider.load_product_list_from_s3()
        self.assertIsInstance(products, list)
        self.assertEqual(products[0]['product_id'], '1')
        self.assertEqual(products[0]['seller_product_id'], '10')

    @patch('projects.tiki.tiki.spiders.prd_detail_crawler.Mailer.send_error_email')
    def test_parse(self, mock_send_error_email):
        response = MagicMock()
        response.text = json.dumps({"id": "1", "categories": {"id": "100"}})
        items = list(self.spider.parse(response))
        self.assertEqual(len(items), 1)
        self.assertIsInstance(items[0], ProductDetail)
        self.assertEqual(items[0]['product_id'], '1')
        self.assertEqual(items[0]['category_id'], '100')
        mock_send_error_email.assert_not_called()

    @patch('projects.tiki.tiki.spiders.prd_detail_crawler.Mailer.send_error_email')
    def test_parse_error(self, mock_send_error_email):
        response = MagicMock()
        response.text = 'invalid json'
        list(self.spider.parse(response))
        mock_send_error_email.assert_called_once()


if __name__ == '__main__':
    unittest.main()
