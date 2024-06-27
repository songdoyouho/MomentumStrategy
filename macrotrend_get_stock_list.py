from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import re
import json

class MacroTrendsScraper:
    def __init__(self):
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("start-maximized")
        self.driver = uc.Chrome(options=self.options)
        self.url = "https://www.macrotrends.net/stocks/stock-screener"

    def open_website(self):
        try:
            self.driver.get(self.url)
            self.wait_for_page_load()
        except Exception as e:
            print(f"無法打開網站: {e}")

    def wait_for_page_load(self):
        try:
            wait = WebDriverWait(self.driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        except Exception as e:
            print(f"頁面加載超時: {e}")

    def extract_stock_list(self):
        try:
            page_source = self.driver.page_source
            match = re.search(r'var originalData = (\[.*?\]);', page_source, re.DOTALL)
            if match:
                original_data = match.group(1)
                print("Extracted originalData.")
                original_data_list = json.loads(original_data)
                return original_data_list
            else:
                print("originalData not found in the page source.")
                return None
        except Exception as e:
            print(f"數據提取失敗: {e}")
            return None

    def close(self):
        self.driver.quit()

# 使用範例
if __name__ == "__main__":
    scraper = MacroTrendsScraper()
    scraper.open_website()
    stock_list = scraper.extract_stock_list()
    if stock_list:
        print(stock_list[0].keys(), len(stock_list))
        print(stock_list[0]['ticker'], stock_list[0]['comp_name'], stock_list[0]['comp_name_2'])
    scraper.close()