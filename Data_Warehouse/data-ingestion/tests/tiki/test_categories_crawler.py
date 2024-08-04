import unittest
from unittest.mock import patch, MagicMock
import json
from projects.tiki.tiki.spiders.categories_crawler import TikiCrawlerSpider


class TestTikiCrawlerSpider(unittest.TestCase):

    def setUp(self):
        self.spider = TikiCrawlerSpider()

    @patch('projects.tiki.tiki.spiders.categories_crawler.Mailer.send_error_email')
    def test_parse_success(self, mock_send_error_email):
        """
        Test the parse method to ensure it correctly filters and processes categories.
        """
        response = MagicMock()
        response.body = json.dumps({
            "menu_block": {
                "items": [
                    {"text": "thời trang nam", "link": "/c/thoi-trang-nam/c1"},
                    {"text": "electronics", "link": "/c/electronics/c2"}
                ]
            }
        }).encode('utf-8')
        requests = list(self.spider.parse(response))
        self.assertEqual(len(requests), 1)
        self.assertIn('_meta', requests[0].__dict__)
        self.assertEqual(requests[0].meta['parent_id'], '1')
        mock_send_error_email.assert_not_called()

    @patch('projects.tiki.tiki.spiders.categories_crawler.Mailer.send_error_email')
    def test_parse_error(self, mock_send_error_email):
        """
        Test the parse method to ensure it sends an error email on invalid JSON.
        """
        response = MagicMock()
        response.body = 'invalid json'.encode('utf-8')
        list(self.spider.parse(response))
        mock_send_error_email.assert_called_once()

    def test_filter_categories(self):
        """
        Test the filter_categories method to ensure it correctly filters out unwanted categories.
        """
        categories = [
            {"text": "thời trang nam", "link": "/c/thoi-trang-nam/c1"},
            {"text": "electronics", "link": "/c/electronics/c2"},
            {"text": "thời trang nữ", "link": "/c/thoi-trang-nu/c2"}

        ]
        filtered = self.spider.filter_categories(categories)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['text'], 'thời trang nam')
        self.assertEqual(filtered[1]['text'], 'thời trang nữ')

    def test_create_category_requests(self):
        """
        Test the create_category_requests method to ensure it creates the correct Scrapy requests.
        """
        cat = {"text": "thời trang nữ", "link": "thoi-trang-nu/c2"}
        requests = list(self.spider.create_category_requests(cat))
        self.assertEqual(len(requests), 1)
        self.assertIn('_meta', requests[0].__dict__)
        self.assertEqual(requests[0].meta['parent_id'], '2')
        self.assertEqual(requests[0].meta['parent_name'], 'thời trang nữ')
        self.assertEqual(requests[0].meta['parent_key'], 'thoi-trang-nu')

    @patch('projects.tiki.tiki.spiders.categories_crawler.Mailer.send_error_email')
    def test_parse_sub_categories(self, mock_send_error_email):
        """
        Test the parse_sub_categories method to ensure it correctly parses subcategories and creates the expected items.
        """
        response = MagicMock()
        response.meta = {
            'parent_id': '2',
            'parent_name': 'thời trang nữ',
            'parent_key': 'thoi-trang-nu'
        }
        response.body = json.dumps({
            "data": [
                {"id": "21", "name": "áo dài", "url_key": "ao-dai"},
                {"id": "22", "name": "áo sơ mi", "url_key": "ao-so-mi"}
            ]
        }).encode('utf-8')
        items = list(self.spider.parse_sub_categories(response))
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item['category_id'], '2')
        self.assertEqual(item['category_name'], 'thời trang nữ')
        self.assertEqual(item['category_urlKey'], 'thoi-trang-nu')
        self.assertEqual(len(item['children']), 2)
        self.assertEqual(item['children'][0]['category_id'], '21')
        self.assertEqual(item['children'][0]['category_name'], 'áo dài')
        self.assertEqual(item['children'][0]['category_urlKey'], 'ao-dai')
        self.assertEqual(item['children'][1]['category_id'], '22')
        self.assertEqual(item['children'][1]['category_name'], 'áo sơ mi')
        self.assertEqual(item['children'][1]['category_urlKey'], 'ao-so-mi')
        mock_send_error_email.assert_not_called()


if __name__ == '__main__':
    unittest.main()
