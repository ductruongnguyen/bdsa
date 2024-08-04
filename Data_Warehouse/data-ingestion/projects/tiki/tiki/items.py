# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst


def to_str(value):
    return str(value)


class CategoryItem(scrapy.Item):
    category_id = scrapy.Field(output_processor=Join())
    category_name = scrapy.Field(output_processor=Join())
    category_urlKey = scrapy.Field(output_processor=Join())
    children = scrapy.Field()


class URLItem(scrapy.Item):
    url = scrapy.Field(output_processor=Join())
    total_page = scrapy.Field(input_processor=MapCompose(int))


class ProductList(scrapy.Item):
    product_id = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    seller_product_id = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    timestamp = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())


class ProductDetail(scrapy.Item):
    product_id = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    category_id = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    data = scrapy.Field(output_processor=TakeFirst())
    timestamp = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
