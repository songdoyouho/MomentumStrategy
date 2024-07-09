import time
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import csv
from bs4 import BeautifulSoup

# 初始化瀏覽器
options = uc.ChromeOptions()
driver = uc.Chrome(options=options)

# 進入目標網站
url = "https://www.tpex.org.tw/web/regular_emerging/deListed/de-listed_companies.php?l=zh-tw"
driver.get(url)

try:
    # 等待 "請選擇下櫃年份" 下拉框可見並選擇 "全部"
    year_dropdown = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "select[name='select_year']"))
    )
    year_dropdown.click()

    # 選擇 "全部" 選項
    all_option = WebDriverWait(driver, 20).until(
        EC.visibility_of_element_located((By.XPATH, "//select[@name='select_year']/option[text()='全部']"))
    )
    all_option.click()

    # 等待下拉選單選項完成選擇
    time.sleep(1)

    # 點擊查詢按鈕
    query_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@value='查詢']"))
    )
    query_button.click()

    # 等待結果加載
    time.sleep(5)

    # 使用 BeautifulSoup 解析頁面
    def parse_table(page_source):
        soup = BeautifulSoup(page_source, 'html.parser')
        result_table = soup.find('table', {'class': 'page-table'})
        table_data = []

        if result_table:
            # 抓取表格標題
            headers = [header.text for header in result_table.find_all('td', {'class': 'page-table-head'})]

            # 抓取表格內容
            rows = result_table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if cols:  # 確保不處理空行
                    table_data.append([col.text.strip() for col in cols])

        return headers, table_data[1:]

    # 初始頁面抓取
    headers, all_data = parse_table(driver.page_source)

    # 將表格內容寫入 CSV 文件
    with open('delisted_stock_in_OTC_market.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(all_data)

        # 遍歷所有頁面
        page_number = 1
        while page_number < 57:  # 停止搜尋超過 57 頁
            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@class='table-text-over' and contains(text(),'下一頁')]"))
                )
                next_button.click()

                # 等待新頁面加載
                time.sleep(5)

                # 解析新頁面並附加數據
                _, page_data = parse_table(driver.page_source)
                writer.writerows(page_data)

                page_number += 1

            except:
                break

    print("所有頁面表格內容已成功抓取並保存到 de_listed_companies.csv")

finally:
    # 關閉瀏覽器
    driver.quit()
