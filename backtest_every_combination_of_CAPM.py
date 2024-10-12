import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from portfolio import TradingSystem
from sort_residual_momentum import get_backtest_date
from date_manegement.utils import *

def initialize_data(MomentumSlidingWindow, ResidualMomentumSlidingWindow):
    """
    初始化回測所需的數據
    
    參數:
    MomentumSlidingWindow: 動量滑動窗口大小
    ResidualMomentumSlidingWindow: 殘差動量滑動窗口大小
    
    返回:
    date_list: 日期列表
    database_dict: 數據庫字典
    stock_id_list: 股票ID列表
    trading_system: 交易系統實例
    daliy_median: 每日中位數
    sorted_stock_id_residual_momentum_dict: 排序後的股票殘差動量字典
    first_day_in_a_month: 每月第一個交易日
    """
    date_list, database_dict, stock_id_list = get_backtest_date()
    trading_system = TradingSystem(database_dict, date_list)
    daliy_median = get_daliy_median(date_list, stock_id_list, database_dict)
    sorted_stock_id_residual_momentum_dict = get_and_sort_residual_momentum(MomentumSlidingWindow, ResidualMomentumSlidingWindow)
    first_day_in_a_month = get_first_day_and_yesterday(next(iter(sorted_stock_id_residual_momentum_dict)))
    return date_list, database_dict, stock_id_list, trading_system, daliy_median, sorted_stock_id_residual_momentum_dict, first_day_in_a_month

def get_and_sort_residual_momentum(MomentumSlidingWindow, ResidualMomentumSlidingWindow):
    """
    獲取並排序殘差動量
    
    參數:
    MomentumSlidingWindow: 動量滑動窗口大小
    ResidualMomentumSlidingWindow: 殘差動量滑動窗口大小
    
    返回:
    排序後的股票殘差動量字典
    """
    sorted_stock_id_residual_momentum_dict = get_residual_momentum(MomentumSlidingWindow, ResidualMomentumSlidingWindow)
    sorted_dates = sorted(sorted_stock_id_residual_momentum_dict.keys(), key=lambda x: datetime.strptime(x, '%Y-%m-%d'))
    return {date: sorted_stock_id_residual_momentum_dict[date] for date in sorted_dates}

def filter_stocks_by_trading_volume(sorted_stock_id_residual_momentum, database_dict, yesterday, monthly_median_value):
    """
    過濾掉交易量比較小的股票
    
    參數:
    sorted_stock_id_residual_momentum: 排序後的股票殘差動量字典
    database_dict: 包含股票數據的字典
    yesterday: 前一天的日期
    monthly_median_value: 月度中位數值
    
    返回:
    filtered_sorted_stock_id_residual_momentum: 過濾後的股票殘差動量字典
    """
    filtered_sorted_stock_id_residual_momentum = {}
    for stock_id, momentum in sorted_stock_id_residual_momentum.items():
        trading_money = database_dict[stock_id][yesterday]['trading_money']
        if trading_money is not None and trading_money > monthly_median_value:
            filtered_sorted_stock_id_residual_momentum[stock_id] = momentum
    return filtered_sorted_stock_id_residual_momentum

def close_all_positions(trading_system, database_dict, date_list, first_date):
    """
    平倉所有持倉
    
    參數:
    trading_system: 交易系統實例
    database_dict: 數據庫字典
    date_list: 日期列表
    first_date: 當前月份的第一個交易日
    
    返回:
    monthly_profit_percent: 月度收益率列表
    monthly_profit_amount: 月度收益金額列表
    """
    monthly_profit_percent = []
    monthly_profit_amount = []
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
    return monthly_profit_percent, monthly_profit_amount

def calculate_monthly_results(monthly_profit_percent, monthly_profit_amount, first_date, total_values, dates, trading_system):
    """
    計算月度交易結果
    
    參數:
    monthly_profit_percent: 月度收益率列表
    monthly_profit_amount: 月度收益金額列表
    first_date: 當前月份的第一個交易日
    total_values: 總資產值列表
    dates: 日期列表
    trading_system: 交易系統實例
    
    返回:
    monthly_profit_percent: 更新後的月度收益率列表
    monthly_profit_amount: 更新後的月度收益金額列表
    """
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
    
    return monthly_profit_percent, monthly_profit_amount

def execute_long_trades(top_10, first_date, yesterday, database_dict, date_list, portfolio_split, trading_system):
    """
    執行做多交易
    
    參數:
    top_10: 前10名股票列表
    first_date: 當前月份的第一個交易日
    yesterday: 前一個交易日
    database_dict: 數據庫字典
    date_list: 日期列表
    portfolio_split: 每個股票的投資金額
    trading_system: 交易系統實例
    """
    for stock_id in top_10:
        print(stock_id, first_date)
        valid_date_in_future, open_price = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
        valid_date_in_before, close_price = find_valid_date_in_before(database_dict, stock_id, date_list, yesterday)
        try:
            stock_quantity = calculate_stock_quantity(portfolio_split, open_price, 0.001425) 
        except:
            stock_quantity = calculate_stock_quantity(portfolio_split, close_price, 0.001425) 
        
        if stock_quantity > 0 and open_price is not None and valid_date_in_future is not None:
            stock_price = database_dict[stock_id][valid_date_in_future]['open']
            trading_system.long_stock(valid_date_in_future, stock_id, stock_price, stock_quantity, 'buy')
        else:
            stock_price = database_dict[stock_id][valid_date_in_before]['open']
            trading_system.long_stock(valid_date_in_before, stock_id, stock_price, stock_quantity, 'buy')

def execute_short_trades(bottom_10, first_date, yesterday, database_dict, date_list, portfolio_split, trading_system):
    """
    執行做空交易
    
    參數:
    bottom_10: 後10名股票列表
    first_date: 當前月份的第一個交易日
    yesterday: 前一個交易日
    database_dict: 數據庫字典
    date_list: 日期列表
    portfolio_split: 每個股票的投資金額
    trading_system: 交易系統實例
    """
    for stock_id in bottom_10:
        valid_date_in_future, open_price = find_next_valid_date_in_future(database_dict, stock_id, date_list, first_date)
        valid_date_in_before, close_price = find_valid_date_in_before(database_dict, stock_id, date_list, yesterday)
        try:
            stock_quantity = calculate_stock_quantity(portfolio_split, open_price, 0.001425)
        except:
            stock_quantity = calculate_stock_quantity(portfolio_split, close_price, 0.001425)
        
        if stock_quantity > 0 and open_price is not None and valid_date_in_future is not None:
            stock_price = database_dict[stock_id][valid_date_in_future]['open']
            trading_system.short_stock(valid_date_in_future, stock_id, stock_price, stock_quantity, 'sell')
        else:
            stock_price = database_dict[stock_id][valid_date_in_before]['open']
            trading_system.short_stock(valid_date_in_before, stock_id, stock_price, stock_quantity, 'sell')

def calculate_and_print_trading_results(trading_system, output_xlsx_name):
    """
    計算並打印交易結果
    
    參數:
    trading_system: 交易系統實例
    output_xlsx_name: 輸出的 Excel 檔案名稱
    """
    total_profit_loss = 0
    total_return_rate = 0
    total_trades = 0
    profitable_trades = 0
    losing_trades = 0

    for trade in trading_system.trade_log:
        if trade['action'] in ['sell', 'buy'] and 'profit_amount' in trade:
            total_profit_loss += trade['profit_amount']
            total_return_rate += trade['profit_percent']
            total_trades += 1
            if trade['profit_amount'] > 0:
                profitable_trades += 1
            else:
                losing_trades += 1

    average_return_rate = total_return_rate / total_trades if total_trades > 0 else 0
    win_rate = (profitable_trades / total_trades) * 100 if total_trades > 0 else 0

    print("交易結果摘要：")
    print(f"總盈虧：{total_profit_loss:.2f}")
    print(f"平均報酬率：{average_return_rate:.2f}%")
    print(f"總交易次數：{total_trades}")
    print(f"獲利交易次數：{profitable_trades}")
    print(f"虧損交易次數：{losing_trades}")
    print(f"勝率：{win_rate:.2f}%")

    # 創建一個包含交易結果摘要的字典
    summary_data = {
        "總盈虧": [f"{total_profit_loss:.2f}"],
        "平均報酬率": [f"{average_return_rate:.2f}%"],
        "總交易次數": [total_trades],
        "獲利交易次數": [profitable_trades],
        "虧損交易次數": [losing_trades],
        "勝率": [f"{win_rate:.2f}%"]
    }

    trading_system.export_trade_log_to_excel(output_xlsx_name, summary_data) # 應該帶入每次測試的參數當檔名

def plot_total_value_over_time(dates, total_values):
    """
    繪製總資產變化圖
    
    參數:
    dates: 日期列表
    total_values: 總資產值列表
    """
    plt.figure(figsize=(10, 6))
    plt.plot(dates, total_values, marker='o', linestyle='-', color='b')
    plt.xlabel('Date')
    plt.ylabel('Total Asset')
    plt.title('Total Asset Over Time')

    plt.axhline(y=0, color='r', linestyle='--')

    min_value = min(total_values)
    max_value = max(total_values)
    plt.ylim(min_value, max_value)
    plt.yticks(np.linspace(min_value, max_value, 10))

    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

    plt.xticks(ticks=range(0, len(dates), 10), rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    # 問題： stock_quantity 為 0 時，賣的時候會出現問題 => 在第一次買/賣的時候先檢查 stock_quantity 是否大於 0
    # 找 future price 的時候，如果沒有資料，要怎麼處理? 這會導致下個 loop 賣不掉這個持股 => 如果 future price 是 None，則往前找

    for MomentumSlidingWindow in range(500, 1001, 40):
        for ResidualMomentumSlidingWindow in range(40, min(MomentumSlidingWindow, 981), 40):
            date_list, database_dict, stock_id_list, trading_system, daliy_median, sorted_stock_id_residual_momentum_dict, first_day_in_a_month = initialize_data(MomentumSlidingWindow, ResidualMomentumSlidingWindow)

            total_values = []
            dates = []

            # 拿到每天的 trading money 的中位數
            daliy_median = get_daliy_median(date_list, stock_id_list, database_dict)

            # 拿到每天的 residual momentum 然後排序，存成 dictionary
            sorted_dates = sorted(sorted_stock_id_residual_momentum_dict.keys(), key=lambda x: datetime.strptime(x, '%Y-%m-%d'))
            sorted_dict = {date: sorted_stock_id_residual_momentum_dict[date] for date in sorted_dates}
            sorted_stock_id_residual_momentum_dict = sorted_dict

            first_date = None
            for date in sorted_stock_id_residual_momentum_dict.keys():
                first_date = date
                break

            # 拿到每個月的第一個開盤日日期，以及前一天的開盤日日期
            first_day_in_a_month = get_first_day_and_yesterday(first_date)

            monthly_profit_percent = []
            monthly_profit_amount = []

            # 遍歷每個月的開盤日
            for day in first_day_in_a_month:
                print(day)
                first_date = day[0] # 這個月的開盤日
                yesterday = day[1] # 這個月開盤日的前一天開盤日
                sorted_stock_id_residual_momentum = sorted_stock_id_residual_momentum_dict[yesterday] # 前一天的殘差動量排序
                monthly_median_value = get_previous_month_average_median(yesterday, daliy_median) # 前一個月的中位數

                # 過濾掉交易量比較小的股票
                filtered_sorted_stock_id_residual_momentum = filter_stocks_by_trading_volume(
                    sorted_stock_id_residual_momentum,
                    database_dict,
                    yesterday,
                    monthly_median_value
                )
                    
                # 找出當天 residual momentum 最前十及最後十的股票
                top_10 = []
                bottom_10 = []
                for i, (key, value) in enumerate(filtered_sorted_stock_id_residual_momentum.items()):
                    if i < 10:
                        top_10.append(key)
                    if i > len(filtered_sorted_stock_id_residual_momentum) - 11:
                        bottom_10.append(key)

                portfolio = trading_system.portfolio
                # 處理前一個月的持股，通通平倉，並計算損益
                monthly_profit_percent, monthly_profit_amount = close_all_positions(trading_system, database_dict, date_list, first_date)
                monthly_profit_percent, monthly_profit_amount = calculate_monthly_results(monthly_profit_percent, monthly_profit_amount, first_date, total_values, dates, trading_system)
                # 重新設定初始資金
                trading_system.initial_money = 20000000
                portfolio_split = 1000000
                
                execute_long_trades(bottom_10, first_date, yesterday, database_dict, date_list, portfolio_split, trading_system)
                execute_short_trades(top_10, first_date, yesterday, database_dict, date_list, portfolio_split, trading_system)

                # 顯示目前投資組合
                trading_system.show_portfolio(first_date, 'open')
                print("---------------------------------------------------------------------------")

            # 統計整個回測的結果並存成 xlsx
            output_xlsx_name = f"/Users/youtengkai/Desktop/MomentumStrategy/output_CAPM_results/CAPM_MSW_{MomentumSlidingWindow}_RMSW_{ResidualMomentumSlidingWindow}.xlsx"
            calculate_and_print_trading_results(trading_system, output_xlsx_name)

            # 畫出總資產變化圖
            # plot_total_value_over_time(dates, total_values)