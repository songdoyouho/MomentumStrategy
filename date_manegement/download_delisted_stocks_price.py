import json
import requests
import pandas as pd
from datetime import datetime

def get_stock_price(stock_id, end_date):
    print("processing " + stock_id + "...")
    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": "1994-10-01",
        "end_date": end_date,
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wNy0wNyAxMzowNToyOCIsInVzZXJfaWQiOiJzb25nZG95b3VobyIsImlwIjoiMS4xNjkuMjE0LjEwMiJ9.1jtpnpU0rR2XpPUEYEkbZTDbdwY53sZGXP7hEPW0Nbo",  # 請填入你的 API 金鑰
    }
    
    try:
        resp = requests.get(url, params=parameter)
        resp.raise_for_status()  # 檢查 HTTP 狀態碼
        data = resp.json()
        if "data" in data and data["data"]:
            # save the data to json file
            with open(f"stock_price_data/{stock_id}.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"數據已成功保存 for {stock_id}")
        else:
            print(f"No data returned for {stock_id}")
            failed_stocks.append(stock_id)
    except requests.RequestException as e:
        print(f"Failed to fetch data for {stock_id}: {e}")
        failed_stocks.append(stock_id)

# 讀取 CSV 文件
csv_file_path = '../stock_information/delisted_stock_in_OTC_market.csv'
delisted_stocks = pd.read_csv(csv_file_path)

print(delisted_stocks)

# 提取股票代碼
stock_ids = delisted_stocks['股票代號'].astype(str).tolist()

# 設定終止日期為當前日期
end_date = datetime.today().strftime('%Y-%m-%d')

# 記錄失敗的股票
failed_stocks = []

# 抓取每個股票的數據
for stock_id in stock_ids:
    get_stock_price(stock_id, end_date)

# 印出失敗的股票列表
if failed_stocks:
    print("以下股票抓取數據失敗：")
    for stock_id in failed_stocks:
        print(stock_id)
else:
    print("所有股票數據均抓取成功。")
