from tqdm import tqdm
import math
import pymysql
import datetime
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import statsmodels.api as sm
from concurrent.futures import ThreadPoolExecutor, as_completed

MomentumSlidingWindow = 252
ResidualMomentumSlidingWindow = 21

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

def update_residual_momentum_batch(data_batch):
    """
    批量更新資料庫中的殘差動量。
    
    參數：
    - data_batch：包含要更新的數據的列表，每個元素為 (殘差動量, 股票代碼, 日期)。
    """
    connection = pymysql.connect(
        host='localhost', port=3306, user='root', password='',
        database='stocks_price_db', charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            sql = """
            UPDATE stock_data
            SET residual_momentum = %s
            WHERE stock_id = %s AND date = %s
            """
            cursor.executemany(sql, data_batch)
        connection.commit()
    finally:
        connection.close()

def calculate_residual_momentum(asset_returns, market_returns):
    """
    計算殘差動量。
    
    參數：
    - asset_returns：資產收益率。
    - market_returns：市場收益率。
    
    返回：
    - 殘差動量值。
    """
    market_returns_with_const = sm.add_constant(market_returns)
    model = sm.OLS(asset_returns, market_returns_with_const).fit()
    residuals = model.resid
    return sum(residuals[-ResidualMomentumSlidingWindow:])

def process_stock(stock_id, stock_prices, TAIEX_data):
    """
    處理單一股票的數據，計算其殘差動量。
    
    參數：
    - stock_id：股票代碼。
    - stock_prices：股票價格數據。
    - TAIEX_data：台灣加權指數數據。
    
    返回：
    - 包含更新數據的列表，每個元素為 (殘差動量, 股票代碼, 日期)。
    """
    update_batch = []
    for i in range(MomentumSlidingWindow, len(stock_prices) + 1):
        sliding_window_data = stock_prices[i - MomentumSlidingWindow:i]
        
        asset_prices = []
        market_prices = []
        
        for date, close in sliding_window_data:
            if date not in TAIEX_data:
                break
            asset_prices.append(close)
            market_prices.append(TAIEX_data[date])
        
        if len(asset_prices) != MomentumSlidingWindow:
            continue
        
        asset_log_returns = np.log(np.array(asset_prices[1:]) / np.array(asset_prices[:-1]))
        market_log_returns = np.log(np.array(market_prices[1:]) / np.array(market_prices[:-1]))
        
        residual_momentum = calculate_residual_momentum(asset_log_returns, market_log_returns)
        update_batch.append((float(residual_momentum), stock_id, sliding_window_data[-1][0]))
    
    return update_batch

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


if __name__ == '__main__':
    add_residual_momentum_column()
    stock_data, TAIEX_data = get_stock_data()
    
    all_updates = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_stock = {executor.submit(process_stock, stock_id, prices, TAIEX_data): stock_id for stock_id, prices in stock_data.items()}
        for future in tqdm(as_completed(future_to_stock), total=len(stock_data), desc="處理股票"):
            stock_id = future_to_stock[future]
            try:
                updates = future.result()
                all_updates.extend(updates)
            except Exception as exc:
                print(f'{stock_id} 生成了一個異常: {exc}')
    
    # 批量更新數據庫
    batch_size = 1000
    for i in range(0, len(all_updates), batch_size):
        batch = all_updates[i:i+batch_size]
        update_residual_momentum_batch(batch)

    print("所有股票的殘差動量已更新完成。")