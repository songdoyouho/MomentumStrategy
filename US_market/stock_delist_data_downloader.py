import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time
import os

class StockDelistDataDownloader:
    def __init__(self, download_dir):
        self.download_dir = download_dir
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("start-maximized")
        self.options.add_argument("--headless")
        self.driver = uc.Chrome(options=self.options)

    def get_download_url(self, ticker):
        # 設定目標 URL
        url = f"https://www.macrotrends.net/assets/php/stock_price_history.php?t={ticker}"
        print(url)
        # 瀏覽器打開 URL
        self.driver.get(url)
        
        try:
            # 取得 HTML 內容
            html_content = self.driver.page_source
            
            # 使用正則表達式來找到目標字串
            pattern = re.compile(r"window\.parent\.location\.href = '(https://www\.macrotrends\.net/assets/php/stock_data_download\.php\?s=[^']+&t=" + ticker + ")';")
            match = pattern.search(html_content)
            
            self.driver.switch_to.default_content()
            
            if match:
                print("get download url: " + match.group(1))
                return match.group(1)
            else:
                raise ValueError("未找到下載 URL")
        except Exception as e:
            print(f"錯誤: {e}")
            return None
    
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
        except Exception as e:
            print(f"下載 CSV 文件時出現錯誤: {e}")

    def close(self):
        # 關閉瀏覽器
        self.driver.quit()

if __name__ == "__main__":
    # 使用範例
    ticker = "ABC"
    download_dir = "C:\\Users\\kai\\Downloads"
    downloader = StockDelistDataDownloader(download_dir)

    try:
        download_url = downloader.get_download_url(ticker)
        downloader.download_csv(download_url)
    finally:
        downloader.close()
