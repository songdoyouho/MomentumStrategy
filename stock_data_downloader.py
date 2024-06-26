import re
from selenium import webdriver
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
import requests
import time 
import os

class StockDataDownloader:
    def __init__(self, stock_name, ticker, download_dir):
        self.stock_name = stock_name
        self.ticker = ticker
        self.download_dir = download_dir
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("start-maximized")
        self.driver = uc.Chrome(options=self.options)

    def get_download_url(self):
        # 設定目標 URL
        url = f"https://www.macrotrends.net/stocks/charts/{self.ticker}/{self.stock_name}/stock-price-history"
        
        # 瀏覽器打開 URL
        self.driver.get(url)
        
        # 切換到 iframe
        iframe = self.driver.find_element(By.XPATH, f'//iframe[@src="https://www.macrotrends.net/assets/php/stock_price_history.php?t={self.ticker}"]')
        self.driver.switch_to.frame(iframe)
        
        # 取得 iframe 的 HTML 內容
        html_content = self.driver.page_source
        
        # 使用正則表達式來找到目標字串
        pattern = re.compile(r"window\.parent\.location\.href = '(https://www\.macrotrends\.net/assets/php/stock_data_download\.php\?s=[^']+&t=" + self.ticker + ")';")
        match = pattern.search(html_content)
        
        self.driver.switch_to.default_content()
        
        if match:
            return match.group(1)
        else:
            raise ValueError("未找到下載 URL")
    
    def download_csv(self, download_url):
        try:
            # 瀏覽器打開下載 URL
            self.driver.get(download_url)
            
            # 等待文件下載完成
            time.sleep(10)  # 等待10秒，以確保下載完成
            
            # 確認文件是否下載成功
            files = os.listdir(self.download_dir)
            downloaded_files = [f for f in files if f.endswith(".csv")]
            if downloaded_files:
                print("CSV 文件下載成功:", downloaded_files)
            else:
                print("未找到下載的 CSV 文件")
        finally:
            # 關閉瀏覽器
            self.driver.quit()

if __name__ == "__main__":
    # 使用範例
    stock_name = "nvidia"
    ticker = "NVDA"
    download_dir = "/Users/youtengkai/Downloads"

    downloader = StockDataDownloader(stock_name, ticker, download_dir)
    download_url = downloader.get_download_url()
    downloader.download_csv(download_url)