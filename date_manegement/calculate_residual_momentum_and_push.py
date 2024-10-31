from tqdm import tqdm
import math
import pymysql
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import statsmodels.api as sm
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product

# MomentumSlidingWindow = 252 # 60 ~ 1000 step 20
# ResidualMomentumSlidingWindow = 21 # 40 ~ 980 step 20
# sql id 上限是 2,153,422,416

def get_stock_data():
    """
    從資料庫獲取股票數據。
    
    返回：
    - stock_data：包含所有股票的字典，每個股票包含日期和收盤價。
    - TAIEX_data：台灣加權指數的字典，以日期為鍵，收盤價為值。
    """
    engine = create_engine(f"mysql+pymysql://{'root'}:{''}@{'localhost'}/{'stocks_price_db'}")
    
    # 使用 pandas 一次性讀取所有數據
    query = "SELECT stock_id, date, close FROM stock_data"
    df = pd.read_sql(query, engine)
    
    # 將數據轉換為所需格式
    TAIEX_data = df[df['stock_id'] == 'TAIEX'].set_index('date')['close'].to_dict()
    stock_data = {stock_id: group[['date', 'close']].values.tolist() for stock_id, group in df.groupby('stock_id')}
    
    return stock_data, TAIEX_data

def calculate_residual_momentum(asset_returns, market_returns):
    market_returns_with_const = sm.add_constant(market_returns)
    model = sm.OLS(asset_returns, market_returns_with_const).fit()
    return model.resid

def calculate_daily_residual_momentum(stock_id, stock_prices, TAIEX_data, MomentumSlidingWindow):
    update_batch = []
    for i in range(MomentumSlidingWindow, len(stock_prices) + 1):
        sliding_window_data = stock_prices[i - MomentumSlidingWindow:i]
        
        asset_prices = []
        market_prices = []
        
        for date, close in sliding_window_data:
            if date not in TAIEX_data:
                continue
            asset_prices.append(close)
            market_prices.append(TAIEX_data[date])
        
        asset_log_returns = np.log(np.array(asset_prices[1:]) / np.array(asset_prices[:-1]))
        market_log_returns = np.log(np.array(market_prices[1:]) / np.array(market_prices[:-1]))
        
        residual = calculate_residual_momentum(asset_log_returns, market_log_returns)

        for ResidualMomentumSlidingWindow in range(40, min(MomentumSlidingWindow + 1, 981), 40):
            residual_momentum = round(sum(residual[ResidualMomentumSlidingWindow - 40:ResidualMomentumSlidingWindow]), 4)
            update_batch.append([residual_momentum, stock_id, sliding_window_data[-1][0], MomentumSlidingWindow, ResidualMomentumSlidingWindow])
    
    return update_batch

def insert_residual_momentum_results(data_batch):
    """
    批量插入殘差動量結果到資料庫。
    
    參數：
    - connection：資料庫連接。
    - data_batch：包含要插入的數據的列表，每個元素為 (殘差動量, 股票代碼, 日期, MomentumSlidingWindow, ResidualMomentumSlidingWindow)。
    """
    connection = pymysql.connect(
        host='localhost', port=3306, user='root', password='',
        database='stocks_price_db', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO residual_momentum_results 
            (residual_momentum, stock_id, date, momentum_sliding_window, residual_momentum_sliding_window)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
    except Exception as e:
        print(f"插入數據時發生錯誤: {e}")
        connection.rollback()
    connection.close()

def create_residual_momentum_results_table():
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',
        database='stocks_price_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            # 創建 residual_momentum_results 表
            sql = """
            CREATE TABLE IF NOT EXISTS residual_momentum_results (
                id INT AUTO_INCREMENT PRIMARY KEY,
                stock_id VARCHAR(10),
                date VARCHAR(10),
                momentum_sliding_window INT,
                residual_momentum_sliding_window INT,
                residual_momentum FLOAT,
                INDEX idx_stock_date (stock_id, date),
                INDEX idx_parameters (momentum_sliding_window, residual_momentum_sliding_window),
                CONSTRAINT fk_stock_date FOREIGN KEY (stock_id, date) 
                    REFERENCES stock_data(stock_id, date) ON DELETE CASCADE ON UPDATE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            """
            cursor.execute(sql)

        connection.commit()
        print("residual_momentum_results 表格已成功創建。")
    except pymysql.MySQLError as e:
        print(f"創建表格時發生錯誤: {e}")
    finally:
        connection.close()

def check_stock_data_table():
    connection = pymysql.connect(
        host='localhost', port=3306, user='root', password='',
        database='stocks_price_db', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW INDEX FROM stock_data WHERE Key_name = 'PRIMARY'")
            primary_key = cursor.fetchall()
            print("stock_data 表的主鍵:", primary_key)
    finally:
        connection.close()

def fix_stock_data_primary_key():
    connection = pymysql.connect(
        host='localhost', port=3306, user='root', password='',
        database='stocks_price_db', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("ALTER TABLE stock_data DROP PRIMARY KEY, ADD PRIMARY KEY (stock_id, date)")
        connection.commit()
        print("stock_data 表的主鍵已修復")
    except pymysql.MySQLError as e:
        print(f"修復主鍵時發生錯誤: {e}")
    finally:
        connection.close()

def process_stock(stock_id, prices, TAIEX_data):
    update_batch = []
    print(f"處理股票: {stock_id}")
    for MomentumSlidingWindow in range(500, 1001, 40):
        print(f"處理股票 {stock_id} 的 MomentumSlidingWindow: {MomentumSlidingWindow}")
        update_batch.extend(calculate_daily_residual_momentum(stock_id, prices, TAIEX_data, MomentumSlidingWindow))
        insert_residual_momentum_results(update_batch)
        update_batch = []
    return update_batch

if __name__ == '__main__':
    check_stock_data_table()
    fix_stock_data_primary_key()
    check_stock_data_table()
    create_residual_momentum_results_table()

    stock_data, TAIEX_data = get_stock_data()

    with ThreadPoolExecutor(max_workers=12) as executor:
        for stock_id, prices in stock_data.items():
            executor.submit(process_stock, stock_id, prices, TAIEX_data)
     