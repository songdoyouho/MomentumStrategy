import os
import json
import pymysql

class DatabaseController(object):
    def create_database_and_tables(self, connection):
        with connection.cursor() as cursor:
            # 創建資料庫
            cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_price_db;")
            cursor.execute("USE stocks_price_db;")
            
            # 創建 stock_data 表格
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    date VARCHAR(10),
                    stock_id VARCHAR(10),
                    trading_volume BIGINT,
                    trading_money BIGINT,
                    open FLOAT,
                    max FLOAT,
                    min FLOAT,
                    close FLOAT,
                    spread FLOAT,
                    trading_turnover INT,
                    PRIMARY KEY (date, stock_id)
                );
            """)
        connection.commit()

    def insert_stock_data_bulk(self, connection, stock_data_list):
        with connection.cursor() as cursor:
            query = """
                INSERT INTO stock_data (date, stock_id, trading_volume, trading_money, open, max, min, close, spread, trading_turnover)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    trading_volume = VALUES(trading_volume),
                    trading_money = VALUES(trading_money),
                    open = VALUES(open),
                    max = VALUES(max),
                    min = VALUES(min),
                    close = VALUES(close),
                    spread = VALUES(spread),
                    trading_turnover = VALUES(trading_turnover);
            """
            batch_size = 500
            for i in range(0, len(stock_data_list), batch_size):
                batch = stock_data_list[i:i + batch_size]
                cursor.executemany(query, batch)
        connection.commit()

if __name__ == '__main__':
    database_controller = DatabaseController()

    # 連接到 MySQL 伺服器
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='stocks_price_db',
        charset='utf8mb4'  # 使用 utf8mb4 支援更多的字符
    )

    json_file_list = os.listdir('stock_price_data')

    try:
        # 創建資料庫和表格
        database_controller.create_database_and_tables(connection)

        for json_file in json_file_list:
            # 載入 JSON 檔案
            with open('stock_price_data/' + json_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # 準備要插入的資料
            stock_data_list = [
                (
                    record['date'],
                    record['stock_id'],
                    record['Trading_Volume'],
                    record['Trading_money'],
                    record['open'],
                    record['max'],
                    record['min'],
                    record['close'],
                    record['spread'],
                    record['Trading_turnover']
                ) for record in data['data'] if record['close'] != 0
            ]
            
            # 批量插入股票資料
            database_controller.insert_stock_data_bulk(connection, stock_data_list)

        # 載入 JSON 檔案
        with open('stock_price_data/' + 'TAIEX.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # 準備要插入的資料
        stock_data_list = [
            (
                record['date'],
                record['stock_id'],
                record['Trading_Volume'],
                record['Trading_money'],
                record['open'],
                record['max'],
                record['min'],
                record['close'],
                record['spread'],
                record['Trading_turnover']
            ) for record in data['data']
        ]
        
        # 批量插入股票資料
        database_controller.insert_stock_data_bulk(connection, stock_data_list)
    finally:
        connection.close()

    print("資料已成功插入資料庫")
