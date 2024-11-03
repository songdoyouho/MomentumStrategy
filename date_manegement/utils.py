import pymysql
from datetime import datetime, timedelta
import math

def get_first_day_and_yesterday(first_date):
    # 連接到 MySQL 資料庫
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
            # 撈取自1996年以後的所有開盤日期
            sql = """
            SELECT `date`
            FROM `stock_data`
            WHERE `date` >= %s
            ORDER BY `date` ASC;
            """
            cursor.execute(sql, (first_date))
            result = cursor.fetchall()

            # 將結果存入列表
            date_list = [row['date'] for row in result]
            
    finally:
        connection.close()

    from datetime import datetime
    from collections import defaultdict

    # 準備一個結果列表
    result = []

    # 追踪每年的每個月
    monthly_first_days = defaultdict(list)

    # 迭代所有日期，找出每個月的第一個開盤日
    for i in range(1, len(date_list)):
        current_date = datetime.strptime(date_list[i], '%Y-%m-%d')
        previous_date = datetime.strptime(date_list[i - 1], '%Y-%m-%d')
        
        # 確認當前日期是該月的第一個開盤日
        if current_date.month != previous_date.month:
            # 保存該月的第一個開盤日和前一天的開盤日
            result.append([current_date.strftime('%Y-%m-%d'), previous_date.strftime('%Y-%m-%d')])

    # 最後一個月的處理
    # (因為最後一個月不會有下個月的數據來觸發檢查)
    final_date = datetime.strptime(date_list[-1], '%Y-%m-%d')
    if final_date.month != datetime.strptime(date_list[-2], '%Y-%m-%d').month:
        result.append([final_date.strftime('%Y-%m-%d'), date_list[-2]])

    return result

def get_daliy_median(date_list, stock_id_list, database_dict):
    """
    計算每個日期的交易金額中位數，並過濾交易金額大於中位數的股票。

    :param date_list: 日期列表
    :param stock_id_list: 股票ID列表
    :param database_dict: 股票數據字典
    :return: 字典，鍵為日期，值為交易金額中位數
    """
    from statistics import median

    # 用於記錄 trading_money 大於當天中位數的股票
    daily_median_trading_money = {}

    # 計算每日 trading_money 中位數並過濾股票
    for date in date_list:
        trading_money_list = []

        for stock_id in stock_id_list:
            if date in database_dict[stock_id]:
                trading_money = database_dict[stock_id][date]['trading_money']
                if trading_money is not None:
                    trading_money_list.append(trading_money)

        # 計算當天的 trading_money 中位數
        if trading_money_list:
            median_trading_money = median(trading_money_list)
            daily_median_trading_money[date] = median_trading_money

    return daily_median_trading_money

def get_residual_momentum(msw, rmsw):
    """
    獲取特定動量滑動窗口和殘差動量滑動窗口的結果。

    參數:
    msw: 動量滑動窗口大小
    rmsw: 殘差動量滑動窗口大小

    返回:
    包含結果的 DataFrame
    """
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='stocks_price_db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            query = """
            SELECT *
            FROM residual_momentum_results
            WHERE momentum_sliding_window = %s
            AND residual_momentum_sliding_window = %s
            """
            cursor.execute(query, (msw, rmsw))
            results = cursor.fetchall()

            sorted_stock_id_residual_momentum_dict = {}
            for row in results:
                if row['date'] not in sorted_stock_id_residual_momentum_dict:
                    sorted_stock_id_residual_momentum_dict[row['date']] = {}
                sorted_stock_id_residual_momentum_dict[row['date']].update({row['stock_id']: row['residual_momentum']})

            for key, value in sorted_stock_id_residual_momentum_dict.items():
                sorted_stock_id_residual_momentum_dict[key] = dict(sorted(value.items(), key=lambda item: item[1], reverse=True))

            return sorted_stock_id_residual_momentum_dict

    finally:
        connection.close()

def get_previous_month_average_median(date, daliy_median):
    current_date = datetime.strptime(date, "%Y-%m-%d")
    one_month_ago = current_date - timedelta(days=30)
    
    previous_month_medians = [
        value for key, value in daliy_median.items()
        if one_month_ago <= datetime.strptime(key, "%Y-%m-%d") < current_date
    ]
    
    if previous_month_medians:
        return sum(previous_month_medians) / len(previous_month_medians)
    else:
        return 0
    
def find_next_valid_date_in_future(database_dict, stock_id, date_list, start_date):
    """
    找到給定日期之後的最近一個有效交易日及其開盤價。

    :param database_dict: 股票數據字典
    :param stock_id: 股票ID
    :param date_list: 日期列表
    :param start_date: 起始日期
    :return: (current_date, open_price) 最近有效交易日及其開盤價
    """
    # print(stock_id, start_date)
    date_index = date_list.index(start_date)
    # print(date_index, len(date_list))
    for i in range(date_index, len(date_list)):
        current_date = date_list[i]
        # print(current_date, stock_id in database_dict, current_date in database_dict[stock_id])
        if stock_id in database_dict and current_date in database_dict[stock_id]:
            open_price = database_dict[stock_id][current_date]['open']
            if open_price is not None:
                return current_date, open_price
    return None, None

def find_valid_date_in_before(database_dict, stock_id, date_list, target_date):
    """
    找到給定日期之前的最近一個有效交易日及其收盤價。

    :param database_dict: 股票數據字典
    :param stock_id: 股票ID
    :param date_list: 日期列表
    :param target_date: 目標日期
    :return: (target_date, close_price) 最近有效交易日及其收盤價
    """
    while target_date not in database_dict[stock_id] and date_list.index(target_date) > 0:
        target_date = date_list[date_list.index(target_date) - 1]
    
    close_price = database_dict[stock_id][target_date]['close']
    return target_date, close_price

def calculate_stock_quantity(portfolio_split, open_price, fee_rate):
    """
    計算考慮手續費後的最大購買股數。

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

# # 輸出結果
# import json

# # 將結果轉換為 JSON 格式
# json_data = json.dumps(result, indent=4)

# # 將 JSON 資料寫入文件
# with open('first_open_day_and_previous_python.json', 'w') as json_file:
#     json_file.write(json_data)

# print("JSON 資料已成功輸出到 'first_open_day_and_previous_python.json'")