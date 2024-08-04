import json
from datetime import datetime

import boto3
from itemadapter import ItemAdapter

from .items import CategoryItem, ProductList, ProductDetail


class S3Pipeline:
    def __init__(self):
        self.product_list = None
        self.product_details = None
        self.category_items = None
        self.base_name = None
        self.pre_path = None
        self.bucket_name = None
        self.s3 = None
        self.chunk_size = 1000
        self.chunk_counter = 1

    def open_spider(self, spider):
        self.s3 = boto3.client('s3')
        self.bucket_name = 'spider.truongnd'
        self.base_name = datetime.now().strftime('%Y-%m-%d')
        self.pre_path = 'tiki/raw'

        self.category_items = []
        self.product_list = []
        self.product_details = []

    def process_item(self, item, spider):
        if isinstance(item, CategoryItem):
            self.category_items.append(ItemAdapter(item).asdict())
        elif isinstance(item, ProductList):
            self.product_list.append(ItemAdapter(item).asdict())
        elif isinstance(item, ProductDetail):
            self.product_details.append({
                'product_id': item['product_id'],
                'category_id': item['category_id'],
                'timestamp': item['timestamp'],
                'data': item['data']
            })
            if len(self.product_details) >= self.chunk_size:
                self.upload_product_details_chunk()

        return item

    def upload_product_details_chunk(self):
        prd_sub_path = 'product'
        product_file_name = f'{self.pre_path}/{prd_sub_path}/details-{self.base_name}-{self.chunk_counter}.json'
        product_details = json.dumps(self.product_details, ensure_ascii=False).encode('utf8')
        self.s3.put_object(Body=product_details, Bucket=self.bucket_name, Key=product_file_name)
        self.product_details = []  # Clear the list after uploading
        self.chunk_counter += 1

    def close_spider(self, spider):
        # Save remaining ProductDetails to S3 if any
        if self.product_details:
            self.upload_product_details_chunk()

        # Save CategoryItem to S3 in JSON format
        cat_sub_path = 'category'
        category_file_name = f'{self.pre_path}/{cat_sub_path}/cat-{self.base_name}.json'
        if self.category_items:
            category_data = json.dumps(self.category_items, ensure_ascii=False).encode('utf8')
            self.s3.put_object(Body=category_data, Bucket=self.bucket_name, Key=category_file_name)

        # Save ProductList to S3 in JSON format
        prd_sub_path = 'product'
        product_list_name = f'{self.pre_path}/{prd_sub_path}/list-{self.base_name}.json'
        if self.product_list:
            product_data = json.dumps(self.product_list, ensure_ascii=False).encode('utf8')
            self.s3.put_object(Body=product_data, Bucket=self.bucket_name, Key=product_list_name)
