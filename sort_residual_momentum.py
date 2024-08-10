import pymysql

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
            # print("Date List:", date_list)

            # 2. 取得每個 `stock_id` 的 `date`, `stock_id`, `close`, `residual_momentum` 並轉換成指定格式
            cursor.execute("SELECT stock_id, date, close, residual_momentum FROM stock_data;")
            data = cursor.fetchall()
            result_dict = {}
            for info in data:
                if info['stock_id'] not in result_dict:
                    result_dict[info['stock_id']] = {}
                result_dict[info['stock_id']][info['date']] = {
                    'close': info['close'],
                    'residual_momentum': info['residual_momentum']
                }

            # print("Result Dictionary:", result_dict['2330'])

            # 3. 取得不重複的 `stock_id` 並存成一個 list
            cursor.execute("SELECT DISTINCT stock_id FROM stock_data;")
            stock_id_list = [row['stock_id'] for row in cursor.fetchall()]
            # print("Stock ID List:", stock_id_list)

    finally:
        connection.close()

    return date_list, result_dict, stock_id_list

if __name__ == '__main__':
    date_list, result_dict, stock_id_list = get_backtest_date()

    # date = '2024-08-01'
    for date in date_list:
        stock_id_residual_momentum = {}
        for stock_id in stock_id_list:
            try:
                if result_dict[stock_id][date]['residual_momentum'] != None:
                    stock_id_residual_momentum[stock_id] = result_dict[stock_id][date]['residual_momentum']
            except:
                print(stock_id + 'do not have data in ' + date)
            
        sorted_stock_id_residual_momentum = dict(sorted(stock_id_residual_momentum.items(), key=lambda item: item[1], reverse=True))
        # print(sorted_stock_id_residual_momentum)

        # 回測
        # 看是要多久算一次 sorted residual momentum，然後根據結果買入/賣出對應的個股