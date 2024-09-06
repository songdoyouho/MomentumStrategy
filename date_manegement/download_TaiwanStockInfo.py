import requests
import json
from datetime import datetime

# API 的 URL 和參數
url = "https://api.finmindtrade.com/api/v4/data"
params = {
    "dataset": "TaiwanStockInfo",
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRlIjoiMjAyNC0wNy0wNyAxMzowNToyOCIsInVzZXJfaWQiOiJzb25nZG95b3VobyIsImlwIjoiMS4xNjkuMjE0LjEwMiJ9.1jtpnpU0rR2XpPUEYEkbZTDbdwY53sZGXP7hEPW0Nbo"
}

# 發送 GET 請求
response = requests.get(url, params=params)

# 檢查請求是否成功
if response.status_code == 200:
    data = response.json()
    # 將數據寫入 JSON 文件
    with open("../stock_information/TaiwanStockInfo.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print("數據已成功保存至 TaiwanStockInfo.json")
else:
    print(f"請求失敗，狀態碼: {response.status_code}")
