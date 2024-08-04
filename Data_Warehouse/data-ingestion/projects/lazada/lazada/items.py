# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import Join, MapCompose, TakeFirst


def to_str(value):
    return str(value)


class LazadaProductItem(scrapy.Item):
    # product_id = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # url = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # image = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # breadcrumb = scrapy.Field()
    # product_name = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # price = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # discount_rate = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # sold_items = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # reviews = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # location = scrapy.Field(input_processor=MapCompose(to_str), output_processor=Join())
    # timestamp = scrapy.Field(input_processor=MapCompose(float), output_processor=TakeFirst())
    products = scrapy.Field()
    main_info = scrapy.Field(output_processor=TakeFirst())
    seo_info = scrapy.Field(output_processor=TakeFirst())
    timestamp = scrapy.Field(input_processor=MapCompose(float), output_processor=TakeFirst())
