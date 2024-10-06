import math
import pymysql
import mplcursors
import matplotlib.pyplot as plt
from portfolio import TradingSystem
from date_manegement.utils import get_first_day_and_yesterday
from datetime import datetime, timedelta
import numpy as np

def get_backtest_date():
    """
    從資料庫中提取回測所需的日期和股票數據。

    :return: (date_list, database_dict, stock_id_list)
        - date_list: 不重複且按時間順序排序的日期列表
        - database_dict: 字典，包含每個股票的數據（交易金額、開盤價、收盤價、殘差動量）
        - stock_id_list: 不重複的股票ID列表
    """
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
            # 1. 取得不重複的 `date` 並存成一個按時間順序排序的 list
            cursor.execute("SELECT DISTINCT date FROM stock_data ORDER BY date ASC;")
            date_list = [row['date'] for row in cursor.fetchall()]

            # 2. 取得每個 `stock_id` 的 `date`, `stock_id`, `close`, `residual_momentum` 並轉換成指定格式
            cursor.execute("SELECT stock_id, trading_money, date, open, close, residual_momentum FROM stock_data;")
            data = cursor.fetchall()
            database_dict = {}
            for info in data:
                if info['stock_id'] not in database_dict:
                    database_dict[info['stock_id']] = {}
                database_dict[info['stock_id']][info['date']] = {
                    'trading_money': info['trading_money'],
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

def find_next_valid_date_in_future(database_dict, stock_id, date_list, start_date):
    """
    找到給定日期之後的最近一個有效交易日及其開盤價。

    :param database_dict: 股票數據字典
    :param stock_id: 股票ID
    :param date_list: 日期列表
    :param start_date: 起始日期
    :return: (current_date, open_price) 最近有效交易日及其開盤價
    """
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

if __name__ == '__main__':
    print("get backtest date")
    date_list, database_dict, stock_id_list = get_backtest_date()
    trading_system = TradingSystem(database_dict, date_list)

    # 用於記錄 total_value 和日期
    total_values = []
    dates = []

    # 先用 trading_money 的中位數過濾掉交易量比較小的股票
    daliy_median = get_daliy_median(date_list, stock_id_list, database_dict)

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

    # 拿到每個月的第一個開盤日日期，以及前一天的開盤日日期
    first_day_in_a_month = get_first_day_and_yesterday()

    monthly_profit_percent = []
    monthly_profit_amount = []

    for day in first_day_in_a_month:
        print(day)
        first_date = day[0] # 這個月的開盤日
        yesterday = day[1] # 這個月開盤日的前一天開盤日
        sorted_stock_id_residual_momentum = sorted_stock_id_residual_momentum_dict[yesterday]

        monthly_median_value = get_previous_month_average_median(yesterday, daliy_median)
        filtered_sorted_stock_id_residual_momentum = {}
        for stock_id, momentum in sorted_stock_id_residual_momentum.items():
            trading_money = database_dict[stock_id][yesterday]['trading_money']
            if trading_money is not None:
                if trading_money > monthly_median_value:
                    filtered_sorted_stock_id_residual_momentum[stock_id] = sorted_stock_id_residual_momentum[stock_id]
            
        # 找出當天 residual momentum 最前十及最後十的股票
        top_10 = []
        bottom_10 = []
        for i, (key, value) in enumerate(filtered_sorted_stock_id_residual_momentum.items()):
            if i < 10:
                top_10.append(key)
            if i > len(filtered_sorted_stock_id_residual_momentum) - 11:
                bottom_10.append(key)

        portfolio = trading_system.portfolio
        if portfolio:
            stock_id_list_in_portfolio = list(trading_system.portfolio.keys())
            for stock_id in stock_id_list_in_portfolio:
                valid_date_in_future, _ = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
                if valid_date_in_future is None:
                    if portfolio[stock_id]['position'] == 'long':
                        stock_price = portfolio[stock_id]['long_price']
                        trading_system.long_stock(valid_date_in_future, stock_id, stock_price, portfolio[stock_id]['long_quantity'], 'sell')
                    elif portfolio[stock_id]['position'] == 'short':
                        stock_price = portfolio[stock_id]['short_price']
                        trading_system.short_stock(valid_date_in_future, stock_id, stock_price, portfolio[stock_id]['short_quantity'], 'buy')
                    monthly_profit_percent.append(trading_system.trade_log[-1]['profit_percent'])
                    monthly_profit_amount.append(trading_system.trade_log[-1]['profit_amount'])
                else:
                    stock_price = database_dict[stock_id][valid_date_in_future]['open']
                    if portfolio[stock_id]['position'] == 'long':
                        trading_system.long_stock(valid_date_in_future, stock_id, stock_price, portfolio[stock_id]['long_quantity'], 'sell')
                    elif portfolio[stock_id]['position'] == 'short':
                        trading_system.short_stock(valid_date_in_future, stock_id, stock_price, portfolio[stock_id]['short_quantity'], 'buy')
                    print(trading_system.trade_log[-1]['profit_percent'], trading_system.trade_log[-1]['profit_amount'])
                    monthly_profit_percent.append(trading_system.trade_log[-1]['profit_percent'])
                    monthly_profit_amount.append(trading_system.trade_log[-1]['profit_amount'])

        # 計算當月的交易結果
        if monthly_profit_percent and monthly_profit_amount:
            avg_monthly_profit_percent = sum(monthly_profit_percent) / len(monthly_profit_percent)
            total_monthly_profit_amount = sum(monthly_profit_amount)
            
            print(f"當月平均收益率: {avg_monthly_profit_percent:.2f}%")
            print(f"當月總收益金額: {total_monthly_profit_amount:.2f}")

            total_values.append(total_monthly_profit_amount)
            dates.append(first_date)
            
            # 重置月度收益列表，為下個月做準備
            monthly_profit_percent = []
            monthly_profit_amount = []
        else:
            print("本月沒有交易記錄")

        # 重新設定初始資金
        trading_system.initial_money = 20000000
        portfolio_split = 1000000
        # 根據過濾後的 top 10 股票進行做多交易
        for stock_id in top_10:
            valid_date_in_future, open_price = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
            valid_date_in_before, close_price = find_valid_date_in_before(database_dict, stock_id, date_list, yesterday)
            stock_quantity = calculate_stock_quantity(portfolio_split, open_price, 0.001425)
            if stock_quantity > 0:
                stock_price = database_dict[stock_id][valid_date_in_future]['open']
                trading_system.long_stock(valid_date_in_future, stock_id, stock_price, stock_quantity, 'buy')

        # 根據過濾後的 bottom 10 股票進行做空交易
        for stock_id in bottom_10:
            valid_date_in_future, open_price = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
            valid_date_in_before, close_price = find_valid_date_in_before(database_dict, stock_id, date_list, yesterday)
            stock_quantity = calculate_stock_quantity(portfolio_split, open_price, 0.001425)
            if stock_quantity > 0:
                stock_price = database_dict[stock_id][valid_date_in_future]['open']
                trading_system.short_stock(valid_date_in_future, stock_id, stock_price, stock_quantity, 'sell')

        # 顯示目前投資組合
        trading_system.show_portfolio(first_date, 'open')
        print("---------------------------------------------------------------------------")

    # 計算所有交易結果
    total_profit_loss = 0
    total_return_rate = 0
    total_trades = 0
    profitable_trades = 0
    losing_trades = 0

    # 遍歷所有交易紀錄
    for trade in trading_system.trade_log:
        if trade['action'] in ['sell', 'buy'] and 'profit_amount' in trade:
            total_profit_loss += trade['profit_amount']
            total_return_rate += trade['profit_percent']
            total_trades += 1
            if trade['profit_amount'] > 0:
                profitable_trades += 1
            else:
                losing_trades += 1

    # 計算平均報酬率和勝率
    average_return_rate = total_return_rate / total_trades if total_trades > 0 else 0
    win_rate = (profitable_trades / total_trades) * 100 if total_trades > 0 else 0

    # 印出交易結果
    print("交易結果摘要：")
    print(f"總盈虧：{total_profit_loss:.2f}")
    print(f"平均報酬率：{average_return_rate:.2f}%")
    print(f"總交易次數：{total_trades}")
    print(f"獲利交易次數：{profitable_trades}")
    print(f"虧損交易次數：{losing_trades}")
    print(f"勝率：{win_rate:.2f}%")
    print(f"最終總資產：{trading_system.show_portfolio(date_list[-1], 'close'):.2f}")

    # 將交易記錄導出到 Excel 文件
    trading_system.export_trade_log_to_excel()

    # 畫出總資產變化圖
    plt.figure(figsize=(10, 6))
    plt.plot(dates, total_values, marker='o', linestyle='-', color='b')
    plt.xlabel('Date')
    plt.ylabel('Total Value')
    plt.title('Total Value Over Time')

    # 添加水平零軸線
    plt.axhline(y=0, color='r', linestyle='--')

    # 修改 Y 軸刻度
    min_value = min(total_values)
    max_value = max(total_values)
    plt.ylim(min_value, max_value)  # 設置 Y 軸範圍
    plt.yticks(np.linspace(min_value, max_value, 10))  # 設置 10 個均勻分布的刻度

    # 格式化 Y 軸標籤
    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

    plt.xticks(ticks=range(0, len(dates), 10), rotation=45)
    plt.tight_layout()  # 自動調整佈局以防止標籤被切掉
    plt.show()