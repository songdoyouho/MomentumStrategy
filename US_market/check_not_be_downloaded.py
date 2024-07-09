import os
import time
from US_market.stock_delist_data_downloader import StockDelistDataDownloader
# 假設 txt 文件名為 'example.txt'
filename = 'snp500_list.txt'

# 讀取文件內容
with open(filename, 'r', encoding='utf-8') as file:
    content = file.read()

# 移除不需要的字符並將內容轉換為列表
data_list = eval(content)

# 列印結果
# print(data_list)

download_dir = "C:\\Users\\kai\\Downloads"

files = os.listdir("C:\\Users\\kai\\Downloads\\macrotrend_csv")
downloaded_files = [f for f in files if f.endswith(".csv")]
for i in range(len(downloaded_files)):
    downloaded_files[i] = downloaded_files[i].strip('.csv').split('_')[3]

results = [item for item in data_list if item not in downloaded_files]
print(results, len(results), len(downloaded_files), len(data_list))

downloader = StockDelistDataDownloader(download_dir)

re_download_fail_list = []

try:
    for result in results:
        print(result)
        download_url = downloader.get_download_url(result)
        downloader.download_csv(download_url)
        time.sleep(5)
finally:
    downloader.close()

print(re_download_fail_list, len(re_download_fail_list))