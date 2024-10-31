import pymysql
import pandas as pd

# https://www.011.idv.tw/Taiex/FTaiex
# taiex.csv 從上面這邊下載

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

# 讀取 taiex.csv 檔案
csv_data = pd.read_csv('..\\stock_information\\taiex.csv')
csv_data['Date'] = pd.to_datetime(csv_data['Date'])

try:
    with connection.cursor() as cursor:
        # 讀取資料庫中的日期和收盤價資料
        cursor.execute("SELECT date, close FROM stock_data WHERE stock_id = 'TAIEX'")
        db_data = cursor.fetchall()
        db_data_df = pd.DataFrame(db_data)
        db_data_df['date'] = pd.to_datetime(db_data_df['date'])

        # 找出資料庫中缺失的日期
        missing_dates = csv_data[~csv_data['Date'].isin(db_data_df['date'])]

        # 準備插入資料
        for _, row in missing_dates.iterrows():
            insert_query = """
                INSERT INTO stock_data (date, stock_id, trading_volume, trading_money, open, max, min, close, spread, trading_turnover)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_tuple = (
                row['Date'].strftime('%Y-%m-%d'),
                'TAIEX',
                None,  # trading_volume
                None,  # trading_money
                None,  # open
                None,  # max
                None,  # min
                row['Point'],  # close
                None,  # spread
                None  # trading_turnover
            )
            cursor.execute(insert_query, data_tuple)

        # 提交更改
        connection.commit()

finally:
    connection.close()

print("資料更新完成！")