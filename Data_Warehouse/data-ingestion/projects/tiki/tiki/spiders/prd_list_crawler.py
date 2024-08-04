import scrapy
import json
from ..items import URLItem, ProductList
from scrapy.loader import ItemLoader
import pandas as pd
import time
import boto3
from scrapy import signals
from ..mailer import Mailer
import datetime


class UrlsCrawlerSpider(scrapy.Spider):
    name = "prd_list_crawler"
    allowed_domains = ["tiki.vn"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(UrlsCrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        return spider

    def start_requests(self):
        try:
            categories = self.load_categories_from_s3()
            url_categories = self.extract_no_children(categories)
            if url_categories:
                for category in url_categories:
                    cat_id = str(category['category_id']).strip()
                    key = str(category['category_urlKey']).strip()
                    url = f'https://tiki.vn/api/v2/products?category={cat_id}&urlKey={key}&page=99&limit=50'
                    yield scrapy.Request(url,
                                         self.parse_initial_page,
                                         meta={'cat_id': cat_id, 'key': key})
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in start_requests: {str(e)}")

    def parse_initial_page(self, response):
        try:
            data = json.loads(response.body)
            cat_id = response.meta['cat_id']
            key = response.meta['key']
            total_page = data['paging']['last_page']

            for page in range(1, total_page + 1):
                url = f'https://tiki.vn/api/v2/products?category={cat_id}&urlKey={key}&page={page}&limit=50'
                yield scrapy.Request(url, self.parse, meta={'url': url})
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in parse_initial_page: {str(e)}")

    def load_categories_from_s3(self) -> dict:
        s3 = boto3.client('s3')
        bucket_name = 'spider.truongnd'
        prefix = 'tiki/raw/category'

        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        cat_files = [obj for obj in objects['Contents'] if str(obj['Key']).split('/')[-1].startswith('cat-')]
        cat_files.sort(key=lambda obj: obj['LastModified'], reverse=True)

        most_recent_file = cat_files[0]
        file_key = most_recent_file['Key']
        file_content = None
        try:
            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = file_obj["Body"].read().decode('utf-8')
        except Exception as e:
            self.logger.error(e)

        return json.loads(file_content)

    def extract_no_children(self, categories) -> list:
        result = []

        for category in categories:
            if 'children' in category:
                result.extend(self.extract_no_children(category['children']))
            else:
                result.append({
                    "category_id": category["category_id"],
                    "category_name": category["category_name"],
                    "category_urlKey": category["category_urlKey"]
                })

        return result

    def parse(self, response):
        try:
            data = json.loads(response.body)
            products = data.get('data', [])

            for product in products:
                item = ItemLoader(item=ProductList())
                item.add_value('product_id', str(product.get('id', '')))
                item.add_value('seller_product_id', str(product.get('seller_product_id', '')))
                item.add_value('timestamp', time.time())
                yield item.load_item()
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in parse: {str(e)}")

    def spider_error(self, failure, response, spider):
        Mailer.send_error_email(self.name, f"An error occurred when processing {response.url}\n{str(failure)}")
