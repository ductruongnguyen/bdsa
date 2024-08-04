import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from shutil import which
from time import perf_counter
from typing import List, Dict, Any
from datetime import datetime
import boto3
import shutil

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


class WebScraper:
    def __init__(self, mhtml_directory: str, output_directory: str, webdriver_pool_size: int = 6):
        self.mhtml_directory = mhtml_directory
        self.output_directory = output_directory
        self.output_list_size_threshold = 10000
        self.batch_counter = 0

        self.base_name = datetime.now().strftime('%Y-%m-%d')

        self.data_queue = Queue()
        self.stop_event = threading.Event()
        self.webdriver_pool = Queue()
        self.webdriver_pool_size = webdriver_pool_size

        self.setup_output_directory()

    def setup_driver(self) -> webdriver.Chrome:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        return webdriver.Chrome(service=Service(which('chromedriver')), options=options)

    def setup_output_directory(self):
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)
        else:
            for filename in os.listdir(self.output_directory):
                file_path = os.path.join(self.output_directory, filename)
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)

    def extract_product_data(self, category: List, product) -> Dict[str, Any]:
        def safe_find_element(xpath: str) -> str:
            try:
                return product.find_element(By.XPATH, xpath).text
            except NoSuchElementException:
                return ''

        def safe_find_attribute(xpath: str, name: str) -> str:
            try:
                return product.find_element(By.XPATH, xpath).get_attribute(name)
            except NoSuchElementException:
                return ''

        url = safe_find_attribute('./a', 'href')
        image = safe_find_attribute("./a/div/div/div[1]//img[@style='object-fit: contain']", 'src')
        title = safe_find_element('./a/div/div/div[2]/div[1]/div[1]/div')

        promotions = []
        try:
            promotion_containers = product.find_elements(By.XPATH, './a/div/div/div[2]/div[1]/div[2]/div')
        except NoSuchElementException:
            promotion_containers = None

        if promotion_containers:
            for promotion in promotion_containers:
                try:
                    inner_promotion = promotion.find_element(By.XPATH, './div/div')
                    promotions.append(inner_promotion.text)
                except NoSuchElementException:
                    promotions.append(promotion.text)

        current_price = safe_find_element('./a/div/div/div[2]/div[2]/div[1]/span[3]')
        original_price = safe_find_element('./a/div/div/div[2]/div[2]/div[2]')
        discount_rate = safe_find_element('./a/div/div/div[2]/div[2]/div[3]/div/span')

        price = {
            'current_price': current_price,
            'original_price': original_price,
            'discount_rate': discount_rate
        }

        sold = safe_find_element('./a/div/div/div[2]/div[3]/div[2]')
        location = safe_find_element("./a/div/div/div[2]/div[4]/div[@aria-label='from']")

        return {
            'category': category,
            'url': url,
            'image': image,
            'title': title,
            'promotions': promotions,
            'price': price,
            'sold': sold,
            'location': location
        }

    def extract_categories(self, driver: webdriver.Chrome, xpath: str) -> Dict[str, Any]:
        try:
            category = driver.find_element(By.XPATH, xpath).text
            href = driver.find_element(By.XPATH, xpath).get_attribute('href')
            cat_id = href.split('.')[-1]
        except NoSuchElementException:
            category = ''
            cat_id = -1
        return {
            'category_id': cat_id,
            'category_name': category
        }

    def extract_information_from_html(self, mhtml_file_path: str) -> None:
        driver = self.webdriver_pool.get()
        try:
            url_path = 'file://' + os.path.abspath(mhtml_file_path)
            driver.get(url_path)
            print(f'Start extracting information from {url_path}')

            category = []
            level_1 = self.extract_categories(driver, "//div[@class='shopee-category-list__main-category']/a")
            level_1['parent_id'] = -1
            category.append(level_1)

            level_2 = self.extract_categories(driver,
                                              "//a[contains(@class, 'shopee-category-list__sub-category--active')]")
            level_2['parent_id'] = level_1['category_id']
            category.append(level_2)

            extracted_data = None
            try:
                products = driver.find_elements(By.XPATH, "//ul[@class='row shopee-search-item-result__items']/li")
                if products:
                    extracted_data = [self.extract_product_data(category, product) for product in products]
            except NoSuchElementException:
                return

            if extracted_data:
                for data in extracted_data:
                    self.data_queue.put(data)

            print(f'Done extracting information from {url_path}')

        except Exception as e:
            print(f'Error extracting data from {mhtml_file_path}: {e}')
        finally:
            self.webdriver_pool.put(driver)

    def save_split_files(self) -> None:
        output_list = []

        while not self.stop_event.is_set() or not self.data_queue.empty():
            try:
                data = self.data_queue.get(timeout=1)
                output_list.append(data)

                if len(output_list) >= self.output_list_size_threshold:
                    self.save_output_file(output_list)
                    output_list.clear()

            except Empty:
                continue

        if output_list:
            self.save_output_file(output_list)

    def save_output_file(self, output_list: List[Dict[str, Any]]) -> None:
        file_path = os.path.join(self.output_directory, f'products-{self.base_name}-{self.batch_counter + 1}.json')
        print(f'OUTPUT - Saving {file_path}')
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(output_list, file, ensure_ascii=False, indent=4)
        self.batch_counter += 1

    def run(self) -> None:
        print(f'Start the executors')
        start = perf_counter()

        mhtml_files = [os.path.join(self.mhtml_directory, f) for f in os.listdir(self.mhtml_directory) if
                       f.endswith('.mhtml')]

        # Start consumer thread
        consumer_thread = threading.Thread(target=self.save_split_files)
        consumer_thread.start()

        # Create a pool of WebDriver instances
        for _ in range(self.webdriver_pool_size):
            self.webdriver_pool.put(self.setup_driver())

        with ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(self.extract_information_from_html, mhtml_file) for mhtml_file in mhtml_files}
            for future in as_completed(futures):
                e = future.exception()
                if e is not None:
                    print(f"Exception occurred: {e}")

        # Wait for all tasks to complete
        executor.shutdown(wait=True)

        # Signal consumer to stop and wait for it to finish
        self.stop_event.set()
        consumer_thread.join()

        # Close all WebDriver instances
        while not self.webdriver_pool.empty():
            driver = self.webdriver_pool.get()
            driver.quit()

        end = perf_counter()
        print(f'Finished in {end - start:.2f} seconds')


if __name__ == "__main__":
    scraper = WebScraper(
        mhtml_directory=r'C:\Users\truon\Desktop\FUNiX\Download_Shopee',
        output_directory='./output'
    )
    scraper.run()
