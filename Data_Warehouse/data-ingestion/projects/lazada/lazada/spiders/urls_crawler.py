import datetime
import json
import math
import time
from shutil import which

import boto3
import scrapy
from scrapy import signals
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import os

from ..mailer import Mailer


class URLsCrawlerSpider(scrapy.Spider):
    name = "urls_crawler"
    allowed_domains = ["www.lazada.vn"]

    chrome_profile_data_path = r'C:\Users\truon\AppData\Local\Google\Chrome\User Data'
    profile_directory = r'Profile 1'

    urls_to_crawl = []
    check_gen_info = []

    def __init__(self, *args, **kwargs):
        super(URLsCrawlerSpider, self).__init__(*args, **kwargs)
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
        spider = super(URLsCrawlerSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_error, signal=signals.spider_error)
        return spider

    def start_requests(self):
        try:
            categories = self.load_categories_from_s3()
            url_categories = self.extract_last_level(categories)
            self.logger.info("Url_categories ===> : %s", url_categories)
            if url_categories:
                for category in url_categories:
                    cat_url = category.get('categoryUrl', '')
                    if cat_url:
                        url_key = str(cat_url).split('/')[-1].strip().lower()
                        if url_key == 'do-lot':
                            continue
                        url = (f'https://www.lazada.vn/{url_key}/?ajax=true&isFirstRequest=true&page=1&spm=a2o4n'
                               f'.searchlistcategory.cate_9_1.1.784025bcJsKSEY')
                        yield scrapy.Request(url, self.parse_initial_page, meta={'url': url, 'url_key': url_key})
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

    def parse_initial_page(self, response):
        try:
            log_urls = []
            url_key = response.meta['url_key']
            self.driver.get(response.url)
            self.driver.implicitly_wait(2)
            self.solving_captcha()

            json_content = self.driver.find_element(By.TAG_NAME, 'pre').text
            data = json.loads(json_content)

            total = int(data.get('mainInfo', {}).get('totalResults', '0'))
            size = int(data.get('mainInfo', {}).get('pageSize', '1'))

            number_of_pages = math.ceil(total / size)

            for page in range(1, number_of_pages + 1):
                url = (f'https://www.lazada.vn/{url_key}/?ajax=true&isFirstRequest=true'
                       f'&page={page}&spm=a2o4n.searchlistcategory.cate_9_1.1.784025bcJsKSEY')
                log_urls.append(url)
                self.urls_to_crawl.append(url)

            log_gen = {'log_total': total,
                       'log_size': size,
                       'log_number_of_pages': number_of_pages,
                       'log_urls': log_urls}

            self.check_gen_info.append(log_gen)

        except Exception as e:
            if self.driver:
                self.driver.quit()
            self.logger.error(f"An error occurred: {str(e)}")
            Mailer.send_error_email(self.name, f"An error occurred in parse_initial_page: {str(e)}")

    def extract_last_level(self, categories) -> list:
        result = []
        categories_l1_to_crawl = ['thời trang & phụ kiện nam', 'thời trang & phụ kiện trẻ em',
                                  'thời trang & phụ kiện nữ', 'thể thao & du lịch']
        categories_l2_to_crawl = ['trang phục nam', 'đồ lót nam',
                                  'trang phục nữ', 'đồ ngủ và nội y',
                                  'trang phục bé trai', 'trang phục bé gái',
                                  'quần áo thể thao bé gái', 'quần áo thể thao bé trai']
        for level_1 in categories:
            if str(level_1['categoryName']).strip().lower() in categories_l1_to_crawl:
                if level_1.get('level2TabList', []):
                    for level_2 in level_1['level2TabList']:
                        if str(level_2['categoryName']).strip().lower() in categories_l2_to_crawl:
                            if level_2.get('level3TabList', []):
                                for level_3 in level_2['level3TabList']:
                                    result.append(level_3)
                            else:
                                result.append(level_2)
                else:
                    result.append(level_1)

        # # Get the current date and time
        # now = datetime.datetime.now()
        #
        # # Create the filename with the current date and time
        # filename = now.strftime("./data/cats_to_crawl_%Y_%m_%d_%H.json")
        #
        # # Write the list of dictionaries to the file in JSON format
        # with open(filename, 'w', encoding='utf-8') as file:
        #     json.dump(result, file, ensure_ascii=False, indent=4)
        #
        # self.logger.info(f"All categories to crawl recorded in logs folder: {filename}")

        return result

    def load_categories_from_s3(self) -> dict:
        s3 = boto3.client('s3')
        bucket_name = 'spider.truongnd'
        prefix = 'lazada/raw/category'

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

    def spider_error(self, failure, response, spider):
        Mailer.send_error_email(self.name, f"An error occurred when processing {response.url}\n{str(failure)}")

    def closed(self, reason):
        if not os.path.exists("./data"):
            os.makedirs("./data")

        with open('./data/urls_to_crawl.txt', 'w') as file:
            for url in self.urls_to_crawl:
                file.write(url + '\n')

        # with open('./logs/check_gen.json', 'w', encoding='utf-8') as file:
        #     json.dump(self.check_gen_info, file, ensure_ascii=False, indent=4)

        if self.driver:
            self.driver.quit()
