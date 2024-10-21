import json
import time
import requests
import pandas as pd

def get_stock_price(stock_id, end_date):
    print("processing " + stock_id + "...")

    url = "https://api.finmindtrade.com/api/v4/data"
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": "1994-10-01",
        "end_date": end_date,
        "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wNy0wNyAxMzowNToyOCIsInVzZXJfaWQiOiJzb25nZG95b3VobyIsImlwIjoiMS4xNjkuMjE0LjEwMiJ9.1jtpnpU0rR2XpPUEYEkbZTDbdwY53sZGXP7hEPW0Nbo", # 參考登入，獲取金鑰
    }
    resp = requests.get(url, params=parameter)
    data = resp.json()
    # save the data to json file
    with open("..\\stock_price_data\\" + stock_id + ".json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print("數據已成功保存")

    # data = pd.DataFrame(data["data"])

if __name__ == "__main__":
    with open("../stock_information/TaiwanStockInfo.json", "r", encoding="utf-8") as f:
        results = json.load(f)

    data = results["data"]

    processing_list = []

    for stock in data:
        if stock["type"] == "twse":
            stock_id = stock["stock_id"]
            end_date = stock["date"]

            if end_date != "None":
                print(stock_id, end_date)
                processing_list.append([stock_id, end_date])

    print(f"總共需要處理的股票數量：{len(processing_list)}")

    start_index = 0
    while start_index < len(processing_list):
        end_index = min(start_index + 600, len(processing_list))
        print(f"正在處理第 {start_index + 1} 到第 {end_index} 個股票")

        for processing_stock in processing_list[start_index:end_index]:
            get_stock_price(processing_stock[0], processing_stock[1])

        start_index = end_index

        if start_index < len(processing_list):
            print("等待一小時後繼續處理下一批股票...")
            time.sleep(3600)  # 等待一小時

    get_stock_price("TAIEX", "2024-10-20")