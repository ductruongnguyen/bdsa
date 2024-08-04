import boto3
import json
from datetime import datetime
from .items import LazadaProductItem
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class LazadaPipeline:
    def __init__(self):
        self.product_list = None
        self.base_name = None
        self.pre_path = None
        self.bucket_name = None
        self.s3 = None
        self.chunk_size = 50
        self.chunk_counter = 1
        # self.chunk_counter = 66

    def open_spider(self, spider):
        self.s3 = boto3.client('s3')
        self.bucket_name = 'spider.truongnd'
        self.base_name = datetime.now().strftime('%Y-%m-%d')
        self.pre_path = 'lazada/raw/product'

        self.product_list = []

    def process_item(self, item, spider):
        if isinstance(item, LazadaProductItem):
            self.product_list.append(ItemAdapter(item).asdict())
            if len(self.product_list) >= self.chunk_size:
                self.upload_product_chunk(spider)

        return item

    def upload_product_chunk(self, spider):
        spider.logger.info("LAZADA UPLOADING TO S3333!!!!!!")
        product_file_name = f'{self.pre_path}/products-{self.base_name}-{self.chunk_counter}.json'
        products = json.dumps(self.product_list, ensure_ascii=False).encode('utf8')
        self.s3.put_object(Body=products, Bucket=self.bucket_name, Key=product_file_name)
        self.product_list = []  # Clear the list after uploading
        self.chunk_counter += 1

    def close_spider(self, spider):
        # Save remaining products if any
        if self.product_list:
            self.upload_product_chunk(spider)
