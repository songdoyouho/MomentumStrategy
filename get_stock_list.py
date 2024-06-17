from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import pymysql
from database import DatabaseController


def get_stock_lists(url: str) -> list:
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    # 設置ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # 打開目標網站
    driver.get(url)

    # 模擬滾動到頁面底部
    scroll_pause_time = 2
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause_time)  # 等待頁面加載
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # 獲取頁面源代碼並使用BeautifulSoup解析
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # 找到 body 標籤
    body = soup.body

    # 找到 body 裡面所有的 tbody 標籤
    tbodies = body.find_all('tbody')

    output_list = []

    # 逐個打印 tbody 的所有 tr 標籤內容
    for index, tbody in enumerate(tbodies):
        rows = tbody.find_all('tr')
        for row_index, row in enumerate(rows):
            tds = row.find_all('td')
            company_name = str(tds[0]).split('"')[1]
            detail_url = str(tds[0]).split('"')[3]
            short_name = company_name.split(' ')[0]
            company_name = company_name[len(short_name) + 1:]
            # 拿到 公司全名 detail網址 公司縮寫
            # print(company_name, detail_url, short_name)
            # print("------------------------------------------------")
            output_list.append([company_name, detail_url, short_name])

    # 關閉瀏覽器
    driver.quit()

    return output_list

def get_stock_historial_price(url: str, short_name: str):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    # 設置ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # 打開目標網站
    driver.get(url)

    # 模擬滾動到頁面底部
    scroll_pause_time = 2
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause_time)  # 等待頁面加載
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # 獲取頁面源代碼並使用BeautifulSoup解析
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # 找到 body 標籤
    body = soup.body

    # 找到 body 裡面所有的 tbody 標籤
    tbodies = body.find_all('tbody')

    output_list = []

    # 逐個打印 tbody 的所有 tr 標籤內容
    for tbody in tbodies:
        rows = tbody.find_all('tr')
        for row in rows:
            tds = str(row.find_all('td'))
            extracted_date = None
            match = re.search(r">([^<]+)<", tds.split(',')[0])
            if match:
                extracted_date = match.group(1).strip()

            tmp = tds.split('"')

            # 確認長度正確檢查 tmp 的大小，如果不是則可能沒有資料
            if len(tmp) == 21:
                adjusted_price = float(tmp[3].strip(' USD'))
                real_price = float(tmp[13].strip(' USD'))
                year = extracted_date.split(' ')[1]
                month = extracted_date.split(' ')[0]
                output_list.append([short_name, year, month, adjusted_price, real_price])

    return output_list


if __name__ == '__main__':
    database_controller = DatabaseController()

    # 連接到 MySQL 伺服器
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='mysql',
        charset='utf8mb4'  # 使用 utf8mb4 支援更多的字符
    )

    database_controller.create_database_and_tables(connection)

    # target_urls = ['https://www.digrin.com/stocks/list/exchanges/nyq', 'https://www.digrin.com/stocks/list/exchanges/nas']
    target_urls = ['https://www.digrin.com/stocks/list/exchanges/nms'] #, 'https://www.digrin.com/stocks/list/exchanges/ngm', 'https://www.digrin.com/stocks/list/exchanges/ncm']
    output_stock_list = []

    for url in target_urls:
        tmp_output_list = get_stock_lists(url)
        output_stock_list += tmp_output_list

    for stock_info in output_stock_list:
        print("processing " + stock_info[2] + " ......")
        url = 'https://digrin.com' + stock_info[1] + 'price'
        output_list = get_stock_historial_price(url, stock_info[2])

        # 將數據寫進 database 裡
        stock_info_list = [
            [stock_info[2], stock_info[0]]
        ]

        database_controller.insert_stock_info_bulk(connection, stock_info_list)
        database_controller.insert_stock_price_bulk(connection, output_list)


### 筆記 使用 DB script 依序讀取股價資料
# SELECT *
# FROM your_table_name
# ORDER BY stock_symbol ASC, year DESC, 
# CASE 
#     WHEN month = 'January' THEN 1
#     WHEN month = 'February' THEN 2
#     WHEN month = 'March' THEN 3
#     WHEN month = 'April' THEN 4
#     WHEN month = 'May' THEN 5
#     WHEN month = 'June' THEN 6
#     WHEN month = 'July' THEN 7
#     WHEN month = 'August' THEN 8
#     WHEN month = 'September' THEN 9
#     WHEN month = 'October' THEN 10
#     WHEN month = 'November' THEN 11
#     WHEN month = 'December' THEN 12
# END DESC;

