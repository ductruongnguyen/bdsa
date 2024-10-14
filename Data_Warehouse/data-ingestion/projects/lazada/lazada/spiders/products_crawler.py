import json
import os
import time
from shutil import which

import scrapy
from scrapy import signals
from scrapy.loader import ItemLoader
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from ..items import LazadaProductItem
from ..mailer import Mailer


class ProductsCrawlerSpider(scrapy.Spider):
    name = "products_crawler"
    allowed_domains = ["www.lazada.vn"]
    chrome_profile_data_path = r'C:\Users\truon\AppData\Local\Google\Chrome\User Data'
    profile_directory = r'Profile 1'

    def __init__(self, *args, **kwargs):
        super(ProductsCrawlerSpider, self).__init__(*args, **kwargs)
        self.driver_path = which('chromedriver')
        self.chrome_options = Options()
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-notifications')
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument(f"--user-data-dir={self.chrome_profile_data_path}")
        self.chrome_options.add_argument(f"--profile-directory={self.profile_directory}")
        self.driver = webdriver.Chrome(service=Service(self.driver_path), options=self.chrome_options)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ProductsCrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        return spider

    def start_requests(self):
        try:
            with open(os.path.abspath('./data/urls_to_crawl.txt'), 'r') as file:
                urls = file.readlines()
            if urls:
                for url in urls:
                    yield scrapy.Request(url, self.parse, meta={'url': url})
        except Exception as e:
            Mailer.send_error_email(self.name, f"An error occurred in start_requests: {str(e)}")

    def is_captcha_present(self):
        try:
            captcha_div = self.driver.find_element(By.XPATH, "//iframe[1]")
            return captcha_div is not None
        except:
            return False

    def solving_captcha(self):
        if self.is_captcha_present():
            self.logger.info("CAPTCHA detected. Please solve the CAPTCHA to continue.")
            while self.is_captcha_present():
                self.logger.info("=====> WAITING TO SOLVE CAPTCHA BY HAND!!!")
                time.sleep(3)  # Wait for 3 seconds before checking again
            self.logger.info("CAPTCHA solved. Resuming crawling.")

    def parse(self, response):
        try:
            self.driver.get(response.url)
            self.driver.implicitly_wait(2)
            self.solving_captcha()
            json_content = self.driver.find_element(By.TAG_NAME, 'pre').text
            data = json.loads(json_content)

            item = ItemLoader(item=LazadaProductItem())
            item.add_value('products', data.get('mods', {}).get('listItems', []))
            item.add_value('main_info', data.get('mainInfo', {}))
            item.add_value('seo_info', data.get('seoInfo', {}))
            item.add_value('timestamp', time.time())
            yield item.load_item()

        except Exception as e:
            if self.driver:
                self.driver.quit()
            self.logger.error(f"An error occurred: {str(e)}")
            Mailer.send_error_email(self.name, f"An error occurred in parse: {str(e)}")

    def spider_error(self, failure, response, spider):
        Mailer.send_error_email(self.name, f"An error occurred when processing {response.url}\n{str(failure)}")

    def closed(self, reason):
        if self.driver:
            self.driver.quit()
