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
        while target_date not in self.database_dict[stock_id] and self.date_list.index(target_date) > 0:
            target_date = self.date_list[self.date_list.index(target_date) - 1]
        
        close_price = self.database_dict[stock_id][target_date]['close']
        return target_date, close_price

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
            cost = stock_price * 1000 * stock_quantity
            transaction_fee = math.floor(cost * 0.001425)
            total_cost = cost + transaction_fee

            if self.initial_money >= total_cost:  # 檢查是否有足夠資金進行購買
                self.initial_money -= total_cost
                if stock_id in self.portfolio:
                    self.portfolio[stock_id]['stock_quantity'] += stock_quantity
                else:
                    self.portfolio[stock_id] = {'stock_price': stock_price, 'stock_quantity': stock_quantity}
                
                # 記錄交易
                self.trade_log.append({
                    'date': date,
                    'stock_id': stock_id,
                    'action': 'buy',
                    'stock_price': stock_price,
                    'stock_quantity': stock_quantity,
                    'total_cost': total_cost,
                    'initial_money': self.initial_money,
                    'total_value': self.show_portfolio(date, 'open')
                })
            else:
                print(f"Not enough money to buy {stock_quantity} units of {stock_id}. Available: {self.initial_money}, Required: {total_cost}")

        elif action == 'sell':
            if stock_id in self.portfolio:
                if self.portfolio[stock_id]['stock_quantity'] >= stock_quantity:
                    revenue = stock_price * 1000 * stock_quantity
                    transaction_fee = math.floor(revenue * 0.004425)
                    total_revenue = revenue - transaction_fee
                    self.initial_money += total_revenue
                    self.portfolio[stock_id]['stock_quantity'] -= stock_quantity
                    if self.portfolio[stock_id]['stock_quantity'] == 0:
                        del self.portfolio[stock_id]
                    
                    # 記錄交易
                    self.trade_log.append({
                        'date': date,
                        'stock_id': stock_id,
                        'action': 'sell',
                        'stock_price': stock_price,
                        'stock_quantity': stock_quantity,
                        'total_revenue': total_revenue,
                        'initial_money': self.initial_money,
                        'total_value': self.show_portfolio(date, 'open')
                    })

    def show_portfolio(self, date, open_or_close):
        total_value = self.initial_money
        for stock_id, portfolio_item in self.portfolio.items():
            valid_date_in_before, _ = self.find_valid_date_in_before(stock_id, date)
            stock_price = self.database_dict[stock_id][valid_date_in_before][open_or_close]
            total_value += portfolio_item['stock_quantity'] * stock_price * 1000
            print(stock_id, portfolio_item['stock_quantity'], stock_price, portfolio_item['stock_quantity'] * stock_price * 1000)
        print("initial money: ", self.initial_money)
        print("total value: ", total_value)

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