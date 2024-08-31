import math
import pandas as pd

class TradingSystem():
    def __init__(self, database_dict, date_list):
        self.initial_money = 10000000  # 初始現金
        self.portfolio = {}  # 股票持倉
        self.database_dict = database_dict  # 歷年的資料
        self.date_list = date_list
        self.trade_log = []  # 新增交易紀錄

    def find_valid_date_in_before(self, stock_id, target_date):
        while target_date not in self.database_dict[stock_id]:
            if target_date in self.date_list:
                target_date = self.date_list[self.date_list.index(target_date) - 1]
            else:
                target_date = self.date_list[-1]
        
        close_price = self.database_dict[stock_id][target_date]['close']
        return target_date, close_price
        # while target_date not in self.database_dict[stock_id] and self.date_list.index(target_date) > 0:
        #     if target_date not in self.date_list or self.date_list.index(target_date) == 0:
        #         target_date = self.date_list[self.date_list.index(target_date) - 1]
        #     else:
        #         target_date = self.date_list[-1]
        
        # close_price = self.database_dict[stock_id][target_date]['close']
        # return target_date, close_price

    def adjust(self, date, stock_id, stock_price, stock_quantity):
        # 檢查 portfolio 裡面的庫存量
        if stock_id in self.portfolio:
            if self.portfolio[stock_id]['stock_quantity'] > stock_quantity:
                sell_quantity = self.portfolio[stock_id]['stock_quantity'] - stock_quantity
                self.long_stock(date, stock_id, stock_price, sell_quantity, 'sell')
            else:
                buy_quantity = stock_quantity - self.portfolio[stock_id]['stock_quantity']
                self.long_stock(date, stock_id, stock_price, buy_quantity, 'buy')
        else:
            self.long_stock(date, stock_id, stock_price, stock_quantity, 'buy')

    def long_stock(self, date, stock_id, stock_price, stock_quantity, action):
        if action == 'buy':
            print('buy', stock_id, stock_price, stock_quantity)
            cost = stock_price * 1000 * stock_quantity
            transaction_fee = math.floor(cost * 0.001425)
            total_cost = cost + transaction_fee

            if self.initial_money >= total_cost:  # 檢查是否有足夠資金進行購買
                self.initial_money -= total_cost
                if stock_id in self.portfolio:
                    self.portfolio[stock_id]['long_quantity'] += stock_quantity
                else:
                    self.portfolio[stock_id] = {'long_price': stock_price, 'long_quantity': stock_quantity}
                
                # 記錄交易
                self.trade_log.append({
                    'date': date,
                    'stock_id': stock_id,
                    'action': 'buy',
                    'stock_price': stock_price,
                    'stock_quantity': stock_quantity,
                    'transaction_fee': transaction_fee,
                    'total_cost': total_cost,
                    'initial_money': self.initial_money,
                    'total_value': self.show_portfolio(date, 'open')
                })
            else:
                print(f"Not enough money to buy {stock_quantity} units of {stock_id}. Available: {self.initial_money}, Required: {total_cost}")

        elif action == 'sell':
            if stock_id in self.portfolio and 'long_quantity' in self.portfolio[stock_id]:
                if self.portfolio[stock_id]['long_quantity'] >= stock_quantity:
                    print('sell', stock_id, stock_price, stock_quantity)
                    revenue = stock_price * 1000 * stock_quantity
                    transaction_fee = math.floor(revenue * 0.004425)
                    total_revenue = revenue - transaction_fee
                    self.initial_money += total_revenue
                    self.portfolio[stock_id]['long_quantity'] -= stock_quantity
                    if self.portfolio[stock_id]['long_quantity'] == 0:
                        del self.portfolio[stock_id]
                    
                    # 記錄交易
                    self.trade_log.append({
                        'date': date,
                        'stock_id': stock_id,
                        'action': 'sell',
                        'stock_price': stock_price,
                        'stock_quantity': stock_quantity,
                        'transaction_fee': transaction_fee,
                        'total_revenue': total_revenue,
                        'initial_money': self.initial_money,
                        'total_value': self.show_portfolio(date, 'open')
                    })

    def short_stock(self, date, stock_id, stock_price, stock_quantity, action):
        if action == 'sell':
            print('sell', stock_id, stock_price, stock_quantity)
            revenue = stock_price * 1000 * stock_quantity
            transaction_fee = math.floor(revenue * 0.004425)
            total_revenue = revenue - transaction_fee

            # 檢查是否有足夠的擔保金進行做空
            if self.initial_money >= total_revenue:  
                self.initial_money += total_revenue
                if stock_id in self.portfolio:
                    self.portfolio[stock_id]['short_quantity'] += stock_quantity
                else:
                    self.portfolio[stock_id] = {'short_price': stock_price, 'short_quantity': stock_quantity}
                
                # 記錄交易
                self.trade_log.append({
                    'date': date,
                    'stock_id': stock_id,
                    'action': 'sell',
                    'stock_price': stock_price,
                    'stock_quantity': stock_quantity,
                    'transaction_fee': transaction_fee,
                    'total_revenue': total_revenue,
                    'initial_money': self.initial_money,
                    'total_value': self.show_portfolio(date, 'open')
                })
            else:
                print(f"Not enough margin to short {stock_quantity} units of {stock_id}. Available: {self.initial_money}, Required: {total_revenue}")

        elif action == 'buy':
            if stock_id in self.portfolio and 'short_quantity' in self.portfolio[stock_id]:
                if self.portfolio[stock_id]['short_quantity'] >= stock_quantity:
                    print('buy', stock_id, stock_price, stock_quantity)
                    cost = stock_price * 1000 * stock_quantity
                    transaction_fee = math.floor(cost * 0.001425)
                    total_cost = cost + transaction_fee
                    self.initial_money -= total_cost
                    self.portfolio[stock_id]['short_quantity'] -= stock_quantity
                    if self.portfolio[stock_id]['short_quantity'] == 0:
                        del self.portfolio[stock_id]
                    
                    # 記錄交易
                    self.trade_log.append({
                        'date': date,
                        'stock_id': stock_id,
                        'action': 'buy',
                        'stock_price': stock_price,
                        'stock_quantity': stock_quantity,
                        'transaction_fee': transaction_fee,
                        'total_cost': total_cost,
                        'initial_money': self.initial_money,
                        'total_value': self.show_portfolio(date, 'open')
                    })
                else:
                    print(f"Not enough short position to cover {stock_quantity} units of {stock_id}.")
            else:
                print(f"No short position found for {stock_id} to cover.")

    def show_portfolio(self, date, open_or_close):
        total_value = self.initial_money
        print("\n投資組合狀況:")
        
        for stock_id, portfolio_item in self.portfolio.items():
            valid_date_in_before, _ = self.find_valid_date_in_before(stock_id, date)
            stock_price = self.database_dict[stock_id][valid_date_in_before][open_or_close]

            # 分別計算多頭和空頭的總價值
            long_value = portfolio_item.get('long_quantity', 0) * stock_price * 1000
            short_value = portfolio_item.get('short_quantity', 0) * stock_price * 1000
            
            total_value += long_value - short_value
            
            print(f"{stock_id} 多頭數量: {portfolio_item.get('long_quantity', 0)}, 空頭數量: {portfolio_item.get('short_quantity', 0)}, 當前價格: {stock_price}, 多頭價值: {long_value}, 空頭價值: {short_value}")

        print("初始現金: ", self.initial_money)
        print("投資組合總價值: ", total_value)

        # 檢查 total_value 是否小於 0，並且在必要時候進行處理
        if total_value < 0:
            print("Warning: Total value is negative, which means bankruptcy.")
            total_value = 0  # 可以選擇將 total_value 設置為 0 或執行其他操作

        return total_value

    def show_trade_log(self):
        for trade in self.trade_log:
            print(trade)

    def export_trade_log_to_excel(self, filename='trade_log.xlsx'):
        df = pd.DataFrame(self.trade_log)
        df.to_excel(filename, index=False)
        print(f"Trade log exported to {filename}")

if __name__ == "__main__":
    # 測試 TradingSystem 類別的功能

    # 假設的歷年數據字典
    database_dict = {
        'AAPL': {
            '2024-01-01': {'open': 100, 'close': 100},
            '2024-01-02': {'open': 110, 'close': 110},
            '2024-01-03': {'open': 120, 'close': 120},
        },
        'GOOG': {
            '2024-01-01': {'open': 100, 'close': 100},
            '2024-01-02': {'open': 110, 'close': 110},
            '2024-01-03': {'open': 120, 'close': 120},
        }
    }

    # 假設的日期列表
    date_list = ['2024-01-01', '2024-01-02', '2024-01-03']

    # 初始化 TradingSystem
    trading_system = TradingSystem(database_dict, date_list)

    # 模擬交易
    print("開始測試交易系統...")

    # 測試買入 AAPL 100 股
    trading_system.long_stock('2024-01-01', 'AAPL', 100, 10, 'buy')

    # 測試賣出 AAPL 50 股
    trading_system.long_stock('2024-01-02', 'AAPL', 110, 10, 'sell')

    # 測試做空 GOOG 10 股
    trading_system.short_stock('2024-01-02', 'GOOG', 110, 10, 'sell')

    # 測試回補 GOOG 5 股
    trading_system.short_stock('2024-01-03', 'GOOG', 120, 10, 'buy')

    # 顯示交易日誌
    print("\n交易日誌:")
    trading_system.show_trade_log()

    # 顯示投資組合
    trading_system.show_portfolio('2024-01-03', 'close')

    # 將交易日誌導出到 Excel
    trading_system.export_trade_log_to_excel('test_trade_log.xlsx')

    print("\n測試完成。")