from US_market.macrotrend_get_stock_list import MacroTrendsScraper
from US_market.stock_data_downloader import StockDataDownloader
import os
import time

def check_csv_exist(ticker):
# 確認文件是否下載成功
    files = os.listdir("C:\\Users\\kai\\Downloads")
    downloaded_files = [f for f in files if f.endswith(".csv")]
    for downloaded_file in downloaded_files:
        if ticker in downloaded_file:
            print(ticker, "found in folder.")
            return False
    return True

filename = 'snp500_list.txt'

# 讀取文件內容
with open(filename, 'r', encoding='utf-8') as file:
    content = file.read()

# 移除不需要的字符並將內容轉換為列表
snp500_list = eval(content)

# 列印結果
print(snp500_list)

# get stock list
scraper = MacroTrendsScraper()
scraper.open_website()
stock_list = scraper.extract_stock_list()
scraper.close()

download_dir = "C:\\Users\\kai\\Downloads"
downloader = StockDataDownloader(download_dir)

try:
    for stock in stock_list:
        if stock['ticker'] in snp500_list and check_csv_exist(stock['ticker']):
            ticker = stock['ticker']
            stock_name = stock['comp_name']
            
            download_url = downloader.get_download_url(ticker, stock_name)
            print(download_url)
            downloader.download_csv(download_url)
            time.sleep(5)
finally:
    downloader.close()

print(len(snp500_list))