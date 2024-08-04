import scrapy
import boto3
import json
import pandas as pd

from ..items import ProductDetail
from itemloaders import ItemLoader
import time
from scrapy import signals
from ..mailer import Mailer


class PrdDetailCrawlerSpider(scrapy.Spider):
    name = "prd_detail_crawler"
    allowed_domains = ["tiki.vn"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(PrdDetailCrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        return spider

    def start_requests(self):
        try:
            for url in self.urls_extractor():
                yield scrapy.Request(url, callback=self.parse, meta={'url': url})
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in start_requests: {str(e)}")

    # parse the crawl urls form url keys
    def urls_extractor(self):
        products = self.load_product_list_from_s3()
        urls_crawl_list = []
        if products:
            for product in products:
                prd_id = product['product_id']
                prd_spid = product['seller_product_id']
                url = f'https://tiki.vn/api/v2/products/{prd_id}?platform=web&spid={prd_spid}'
                urls_crawl_list.append(url)
        return urls_crawl_list

    def load_product_list_from_s3(self):
        s3 = boto3.client('s3')

        bucket_name = 'spider.truongnd'  # specify your S3 bucket name
        prefix = 'tiki/raw/product'  # specify the prefix of your files in S3 bucket

        # List all objects in the bucket
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Filter the ones that start with 'cat'
        prd_files = [obj for obj in objects['Contents'] if str(obj['Key']).split('/')[-1].startswith('list-')]

        # Sort them by the 'LastModified' attribute
        prd_files.sort(key=lambda obj: obj['LastModified'], reverse=True)

        # The most recently uploaded file is the first one in the sorted list
        most_recent_file = prd_files[0]
        file_key = most_recent_file['Key']
        file_content = None
        try:
            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = file_obj["Body"].read().decode('utf-8')
        except Exception as e:
            self.logger.error(e)

        return json.loads(file_content)

    def parse(self, response):
        try:
            data = json.loads(response.text)

            item = ItemLoader(item=ProductDetail())
            item.add_value('product_id', data.get('id'))
            item.add_value('category_id', data.get('categories', {}).get('id', ''))
            item.add_value('timestamp', time.time())
            item.add_value('data', data)
            yield item.load_item()
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in parse: {str(e)}")

    def spider_error(self, failure, response, spider):
        Mailer.send_error_email(self.name, f"An error occurred when processing {response.url}\n{str(failure)}")
