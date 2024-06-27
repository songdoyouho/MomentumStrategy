from macrotrend_get_stock_list import MacroTrendsScraper
from stock_data_downloader import StockDataDownloader

# get stock list
scraper = MacroTrendsScraper()
scraper.open_website()
stock_list = scraper.extract_stock_list()
scraper.close()

download_dir = "/Users/youtengkai/Downloads"

for stock in stock_list:
    ticker = stock['ticker']
    stock_name = stock['comp_name']
    downloader = StockDataDownloader(download_dir)
    download_url = downloader.get_download_url(ticker, stock_name)
    downloader.download_csv(download_url)