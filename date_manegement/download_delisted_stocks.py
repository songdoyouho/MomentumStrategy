from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
import pandas as pd

# 設定 Selenium 的選項
chrome_options = Options()
chrome_options.add_argument("--headless")  # 背景執行瀏覽器
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# 設定 ChromeDriver 的路徑
webdriver_service = Service('/path/to/chromedriver')  # 替換為 ChromeDriver 的實際路徑

options = webdriver.ChromeOptions()
options.add_argument("start-maximized")

# 初始化 WebDriver
# driver = webdriver.Chrome(service=webdriver_service, options=chrome_options)
driver = uc.Chrome(options=options)

# 目標 URL
url = "https://www.twse.com.tw/zh/listed/suspend-listing.html"

# 打開網頁
driver.get(url)

# 等待網頁加載完成
driver.implicitly_wait(10)

# 找到表格的 tbody
tbody = driver.find_element(By.CSS_SELECTOR, 'tbody.is-last-page')

# 提取表格數據
rows = []
for tr in tbody.find_elements(By.TAG_NAME, 'tr'):
    cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, 'td')]
    if cells:
        rows.append(cells)

# 關閉 WebDriver
driver.quit()

# 提取表格標題
headers = ["終止上市日期", "公司名稱", "上市編號"]

# 建立 DataFrame
df = pd.DataFrame(rows, columns=headers)

# 顯示結果
print(df)

# 將 DataFrame 保存為 CSV 文件
df.to_csv('delisted_stocks.csv', index=False, encoding='utf-8-sig')
