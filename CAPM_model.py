import json
import math
import pymysql
import numpy as np
from sklearn.linear_model import LinearRegression

MomentumSlidingWindow = 252
ResidualMomentumSlidingWindow = 21

def get_price_data(stock_id, start_index, sliding_window_length):
    asset_prices = None
    # 連接到 MySQL 伺服器
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
            # 執行 SQL 查詢
            sql = """
            SELECT date, close 
            FROM stock_data 
            WHERE stock_id = %s
            ORDER BY date ASC;
            """
            cursor.execute(sql, (stock_id,))
            
            # 獲取所有結果
            result = cursor.fetchall()
            selected_results = result[start_index:start_index + sliding_window_length]
            asset_prices = [row['close'] for row in selected_results]
            print(asset_prices)
    finally:
        connection.close()

    return asset_prices

def linear_regression(asset_log_returns, market_log_returns):
    asset_log_returns = np.array(asset_log_returns)
    market_log_returns = np.array(market_log_returns)
    N = len(market_log_returns)

    # 重新塑形數據以適應sklearn的要求
    X = market_log_returns.reshape(-1, 1)  # 市場收益
    y = asset_log_returns  # 資產收益

    # 創建並訓練線性回歸模型
    model = LinearRegression()
    model.fit(X[:N], y[:N])

    # 獲取回歸係數
    alpha = model.intercept_
    beta = model.coef_[0]

    print(f"alpha: {alpha}")
    print(f"beta: {beta}")

    # 計算殘差
    predicted_returns = model.predict(X)
    residuals = y - predicted_returns

    print(f"殘差: {residuals}")

    return residuals[-ResidualMomentumSlidingWindow:]


asset_prices = get_price_data('0050', 0, MomentumSlidingWindow + ResidualMomentumSlidingWindow)
market_prices = get_price_data('TAIEX', 0, MomentumSlidingWindow + ResidualMomentumSlidingWindow)

asset_log_returns = [math.log(asset_prices[i+1] / asset_prices[i]) for i in range(MomentumSlidingWindow + ResidualMomentumSlidingWindow - 1)]
market_log_returns = [math.log(market_prices[i+1] / market_prices[i]) for i in range(MomentumSlidingWindow + ResidualMomentumSlidingWindow - 1)]
residual_momentum = linear_regression(asset_log_returns, market_log_returns)