from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re


def get_stock_lists(url: str) -> list:
    # 設置ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

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
            # 拿到 公司全名 detail網址 公司縮寫
            # print(company_name, detail_url, short_name)
            # print("------------------------------------------------")
            output_list.append([company_name, detail_url, short_name])

    # 關閉瀏覽器
    driver.quit()

    return output_list

def get_stock_historial_price(url):
    # 設置ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

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
            adjusted_price = float(tmp[3].strip(' USD'))
            real_price = float(tmp[13].strip(' USD'))

            output_list.append([extracted_date, adjusted_price, real_price])

    return output_list


if __name__ == '__main__':
    target_urls = ['https://www.digrin.com/stocks/list/exchanges/nyq', 'https://www.digrin.com/stocks/list/exchanges/nas']
    output_stock_list = []
    for url in target_urls:
        tmp_output_list = get_stock_lists(url)
        output_stock_list += tmp_output_list

    for stock_info in output_stock_list:
        url = 'https://digrin.com' + stock_info[1] + 'price'
        output_list = get_stock_historial_price(url)
        print(output_list[-1])