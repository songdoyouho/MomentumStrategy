from tqdm import tqdm
import math
import pymysql
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression

# 備用
# date_string = '2024-08-01'
# date_key = datetime.datetime.strptime(date_string, '%Y-%m-%d').date()
# 算一組 Ｍ＋Ｎ 大概是 15 分鐘

MomentumSlidingWindow = 252
ResidualMomentumSlidingWindow = 21

def get_stock_data():
    # 連接到你的 MySQL 資料庫
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='stocks_price_db',
        charset='utf8mb4',  # 使用 utf8mb4 支援更多的字符
        cursorclass=pymysql.cursors.DictCursor
    )

    engine = create_engine(f"mysql+pymysql://{'root'}:{''}@{'localhost'}/{'stocks_price_db'}")

    # 從 stock_data 表中讀取資料
    query = "SELECT stock_id, date, close FROM stock_data WHERE stock_id = 'TAIEX'"
    df = pd.read_sql(query, engine)

    # 將資料轉換成你需要的格式
    for stock_id, group in df.groupby('stock_id'):
        TAIEX_data = group.set_index('date')['close'].to_dict()

    try:
        with connection.cursor() as cursor:
            # 查詢所有數據
            sql = "SELECT stock_id, date, close FROM stock_data"
            cursor.execute(sql)
            result = cursor.fetchall()
            
            # 創建字典
            stock_data = {}
            
            for row in result:
                stock_id = row['stock_id']
                date = row['date']
                close = row['close']
                
                if stock_id not in stock_data:
                    stock_data[stock_id] = []
                
                stock_data[stock_id].append([date, close])
        
        # 打印結果字典
        # print(stock_data)

    finally:
        connection.close()

    return stock_data, TAIEX_data

def add_residual_momentum_column():
    # 連接到你的 MySQL 資料庫
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='stocks_price_db',
        charset='utf8mb4',  # 使用 utf8mb4 支援更多的字符
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # 新增 residual_momentum 欄位
            sql = "ALTER TABLE stock_data ADD COLUMN residual_momentum DOUBLE;"
            cursor.execute(sql)
        connection.commit()
    except pymysql.MySQLError as e:
        print(f"新增欄位時發生錯誤: {e.args}")

def update_residual_momentum(stock_id, date, residual_momentum):
    # 連接到你的 MySQL 資料庫
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='stocks_price_db',
        charset='utf8mb4',  # 使用 utf8mb4 支援更多的字符
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # 構建 SQL 更新語句
            sql = """
            UPDATE stock_data
            SET residual_momentum = %s
            WHERE stock_id = %s AND date = %s
            """
            # 執行 SQL 更新語句
            cursor.execute(sql, (residual_momentum, stock_id, date))
        
        # 提交更改
        connection.commit()
    finally:
        # 關閉連接
        connection.close()

import statsmodels.api as sm

def calculate_residual_momentum(asset_returns, market_returns):
    # 假設有資產和市場指數的回報率數據
    # asset_returns = np.array([0.012, 0.022, 0.018, 0.028, 0.026])  # 資產回報率數據
    # market_returns = np.array([0.01, 0.02, 0.015, 0.03, 0.025])  # 市場指數回報率數據

    # 添加常數項
    market_returns_with_const = sm.add_constant(market_returns)

    # 回歸分析
    model = sm.OLS(asset_returns, market_returns_with_const).fit()
    residuals = model.resid  # 獲取殘差

    # 計算殘差的動能效應（例如：過去六個月的殘差平均回報）
    # 這裡為了示範，假設數據較少，使用3個月的窗口期
    # window = 3
    # residual_momentum = np.convolve(residuals, np.ones(window)/window, mode='valid')

    # print("Beta:", model.params[1])
    # print("Alpha:", model.params[0])
    # print("Residuals:", residuals)
    # print("Residual Momentum:", residual_momentum)

    return sum(residuals[-ResidualMomentumSlidingWindow:])

if __name__ == '__main__':
    # add_residual_momentum_column()
    stock_data, TAIEX_data = get_stock_data()
    for key, val in tqdm(stock_data.items(), desc="Processing stocks"):
        print('Processing ', key)
        for i in range(MomentumSlidingWindow + ResidualMomentumSlidingWindow, len(stock_data[key]) + 1):
            sliding_window_data = stock_data[key][i-(MomentumSlidingWindow + ResidualMomentumSlidingWindow):i]

            # 做出 asset_prices, market_prices
            asset_prices = []
            market_prices = []

            # 檢查在這 sliding window 區間，股票跟大盤是否都有收盤價
            break_flag = False

            for data in sliding_window_data:
                if data[0] not in TAIEX_data:
                    # print(data[0], "not in TAIEX_data.")
                    break_flag = True
                    break

                asset_prices.append(data[1])
                market_prices.append(TAIEX_data[data[0]])                

            if break_flag:
                # print("next round.")  
                continue

            # 檢查 market_prices 與 asset_prices 都是 MomentumSlidingWindow + ResidualMomentumSlidingWindow 個
            assert len(market_prices) == len(asset_prices)
            assert len(asset_prices) == (MomentumSlidingWindow + ResidualMomentumSlidingWindow)

            asset_log_returns = [math.log(asset_prices[i+1] / asset_prices[i]) for i in range(MomentumSlidingWindow + ResidualMomentumSlidingWindow - 1)]
            market_log_returns = [math.log(market_prices[i+1] / market_prices[i]) for i in range(MomentumSlidingWindow + ResidualMomentumSlidingWindow - 1)]

            # 如果跑到這邊表示 ok
            # 接著算 residual momentum
            residual_momentum = calculate_residual_momentum(asset_log_returns, market_log_returns)
            
            # print(key, stock_data[key][i-1], i)
            # print("residual momentum:", residual_momentum)
            update_residual_momentum(key, stock_data[key][i-1][0], float(residual_momentum))