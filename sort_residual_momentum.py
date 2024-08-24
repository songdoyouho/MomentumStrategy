import math
import pymysql
import mplcursors
from portfolio import TradingSystem
from get_first_day_and_yesterday import get_first_day_and_yesterday

def get_backtest_date():
    # 建立資料庫連線
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
            # 1. 取得不重複的 `date` 並存成一個 list
            cursor.execute("SELECT DISTINCT date FROM stock_data;")
            date_list = [row['date'] for row in cursor.fetchall()]

            # 2. 取得每個 `stock_id` 的 `date`, `stock_id`, `close`, `residual_momentum` 並轉換成指定格式
            cursor.execute("SELECT stock_id, date, open, close, residual_momentum FROM stock_data;")
            data = cursor.fetchall()
            database_dict = {}
            for info in data:
                if info['stock_id'] not in database_dict:
                    database_dict[info['stock_id']] = {}
                database_dict[info['stock_id']][info['date']] = {
                    'open': info['open'],
                    'close': info['close'],
                    'residual_momentum': info['residual_momentum']
                }

            # 3. 取得不重複的 `stock_id` 並存成一個 list
            cursor.execute("SELECT DISTINCT stock_id FROM stock_data;")
            stock_id_list = [row['stock_id'] for row in cursor.fetchall()]

    finally:
        connection.close()

    return date_list, database_dict, stock_id_list

def find_valid_date_in_before(database_dict, stock_id, date_list, target_date):
    while target_date not in database_dict[stock_id] and date_list.index(target_date) > 0:
        target_date = date_list[date_list.index(target_date) - 1]
    
    close_price = database_dict[stock_id][target_date]['close']
    return target_date, close_price
def find_next_valid_date_in_future(database_dict, stock_id, date_list, start_date):
    date_index = date_list.index(start_date)
    for i in range(date_index, len(date_list)):
        current_date = date_list[i]
        if stock_id in database_dict and current_date in database_dict[stock_id]:
            open_price = database_dict[stock_id][current_date]['open']
            if open_price is not None:
                return current_date, open_price
    return None, None

def calculate_stock_quantity(portfolio_split, open_price, fee_rate):
    """
    計算考慮手續費後的最大購買股數

    :param portfolio_split: 用於購買單一股票的資金
    :param open_price: 股票的開盤價
    :param fee_rate: 手續費率（例如 0.001 代表 0.1%）
    :return: 最大購買股數
    """
    # 計算不考慮手續費時的最大購買股數
    max_quantity = math.floor(portfolio_split / (open_price * 1000))
    
    # 調整考慮手續費後的最大購買數量
    while max_quantity > 0:
        total_cost = max_quantity * open_price * 1000
        total_fee = total_cost * fee_rate
        if total_cost + total_fee <= portfolio_split:
            break
        max_quantity -= 1
    
    return max_quantity

import matplotlib.pyplot as plt

if __name__ == '__main__':
    print("get backtest date")
    date_list, database_dict, stock_id_list = get_backtest_date()
    trading_system = TradingSystem(database_dict, date_list)

    # 用於記錄 total_value 和日期
    total_values = []
    dates = []

    # 拿到每天的 residual momentum 然後排序，存成 dictionary
    sorted_stock_id_residual_momentum_dict = {}
    for date in date_list:
        stock_id_residual_momentum = {}
        for stock_id in stock_id_list:
            try:
                if database_dict[stock_id][date]['residual_momentum'] is not None:
                    stock_id_residual_momentum[stock_id] = database_dict[stock_id][date]['residual_momentum']
            except:
                pass
            
        sorted_stock_id_residual_momentum = dict(sorted(stock_id_residual_momentum.items(), key=lambda item: item[1], reverse=True))
        sorted_stock_id_residual_momentum_dict[date] = sorted_stock_id_residual_momentum

    first_day_in_a_month = get_first_day_and_yesterday()

    for day in first_day_in_a_month:
        first_date = day[0]
        yesterday = day[1]
        sorted_stock_id_residual_momentum = sorted_stock_id_residual_momentum_dict[yesterday]
        
        top_10 = []
        bottom_10 = []
        for i, (key, value) in enumerate(sorted_stock_id_residual_momentum.items()):
            if i < 10:
                top_10.append(key)
        
            if i > len(sorted_stock_id_residual_momentum) - 11:
                bottom_10.append(key)

        portfolio = trading_system.portfolio
        stocks_not_in_top_10 = set(portfolio.keys()) - set(top_10)

        # 找出 portfolio 裡不在 top 10 的股票
        for stock_id in stocks_not_in_top_10:
            valid_date_in_future, _ = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
            stock_price = database_dict[stock_id][valid_date_in_future]['open']
            trading_system.long_stock(valid_date_in_future, stock_id, stock_price, portfolio[stock_id]['stock_quantity'], 'sell')

        # 以今天的開盤價算總資產
        total_value = trading_system.show_portfolio(first_date, 'open')
        # 記錄 total_value 和日期
        total_values.append(total_value)
        dates.append(first_date)

        # 把總資產切成 10 等份，無條件捨去
        portfolio_split = math.floor(total_value / 10)

        for stock_id in top_10:
            valid_date_in_future, open_price = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
            valid_date_in_before, close_price = find_valid_date_in_before(database_dict, stock_id, date_list, yesterday)
            stock_quantity = calculate_stock_quantity(portfolio_split, open_price, 0.001425)
            trading_system.adjust(valid_date_in_future, stock_id, open_price, stock_quantity)

        trading_system.show_portfolio(first_date, 'open')
        print("---------------------------------------------------------------------------")

    trading_system.export_trade_log_to_excel()

    # 畫圖
    plt.figure(figsize=(10, 6))
    plt.plot(dates, total_values, marker='o', linestyle='-', color='b')
    plt.xlabel('Date')
    plt.ylabel('Total Value')
    plt.title('Total Value Over Time')
    plt.xticks(ticks=range(0, len(dates), 10), rotation=45)
    plt.xticks(rotation=45)
    plt.show()