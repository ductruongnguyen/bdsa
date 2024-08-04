import scrapy
import json
from scrapy.loader import ItemLoader
from ..items import CategoryItem
from scrapy import signals
from ..mailer import Mailer


class TikiCrawlerSpider(scrapy.Spider):
    name = "categories_crawler"
    allowed_domains = ["tiki.vn"]
    start_urls = ["https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(TikiCrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        return spider

    def parse(self, response):
        try:
            payload = json.loads(response.body)
            menu_items = payload['menu_block']['items']
            filtered_categories = self.filter_categories(menu_items)
            for cat in filtered_categories:
                yield from self.create_category_requests(cat)
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in parse(): {str(e)}")

    def filter_categories(self, categories):
        filtered = []
        for cat in categories:
            cat_name = cat['text'].strip().lower()
            if cat_name in ['thời trang nam', 'thời trang nữ']:
                filtered.append(cat)
        return filtered

    def create_category_requests(self, cat):
        cat_name = cat['text']
        cat_url = cat['link']
        url_id = cat_url.split('/')[-1].replace('c', '').strip()
        url_key = cat_url.split('/')[-2].strip()
        api_url = f'https://tiki.vn/api/v2/categories?include=children&parent_id={url_id}'

        if api_url:
            yield scrapy.Request(
                api_url,
                callback=self.parse_sub_categories,
                meta={'parent_id': url_id, 'parent_name': cat_name, 'parent_key': url_key}
            )

    def parse_sub_categories(self, response):
        parent_id = response.meta['parent_id']
        parent_name = response.meta['parent_name']
        parent_key = response.meta['parent_key']

        payload = json.loads(response.body)
        categories = payload['data']

        item_l1 = ItemLoader(item=CategoryItem())
        item_l1.add_value("category_id", str(parent_id))
        item_l1.add_value("category_name", parent_name)
        item_l1.add_value("category_urlKey", parent_key)
        item_l1.add_value("children", self.parse_children(categories))

        yield item_l1.load_item()

    def parse_children(self, categories):
        children_items = []

        for category in categories:
            item = ItemLoader(item=CategoryItem())
            item.add_value("category_id", str(category['id']))
            item.add_value("category_name", category['name'])
            item.add_value("category_urlKey", category['url_key'])

            children = category.get('children', [])
            if children:
                item.add_value("children", self.parse_children(category['children']))
            else:
                item.add_value("children", [])

            children_items.append(item.load_item())

        return children_items

    def spider_error(self, failure, response, spider):
        Mailer.send_error_email(self.name, f"An error occurred when processing {response.url}\n{str(failure)}")