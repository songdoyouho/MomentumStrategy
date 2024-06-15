import pymysql

class DatabaseController(object):
    def create_database_and_tables(connection):
        with connection.cursor() as cursor:
            # 創建資料庫
            cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_db;")
            cursor.execute("USE stocks_db;")
            
            # 創建 stock_list 表格
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_list (
                    stock_symbol VARCHAR(10) PRIMARY KEY,
                    stock_name VARCHAR(100),
                    dividend_yield DECIMAL(5, 2),
                    pe_ratio DECIMAL(10, 2),
                    dividend_frequency INT,
                    dgr DECIMAL(5, 2)
                );
            """)
            
            # 創建 stock_prices 表格
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    stock_symbol VARCHAR(10),
                    year INT,
                    month VARCHAR(10),
                    adjusted_price DECIMAL(10, 2),
                    real_price DECIMAL(10, 2),
                    FOREIGN KEY (stock_symbol) REFERENCES stock_list(stock_symbol)
                );
            """)
        connection.commit()

    def insert_stock_info(connection, stock_symbol, stock_name, dividend_yield, pe_ratio, dividend_frequency, dgr):
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_list (stock_symbol, stock_name, dividend_yield, pe_ratio, dividend_frequency, dgr)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                dividend_yield = VALUES(dividend_yield),
                pe_ratio = VALUES(pe_ratio),
                dividend_frequency = VALUES(dividend_frequency),
                dgr = VALUES(dgr);
            """, (stock_symbol, stock_name, dividend_yield, pe_ratio, dividend_frequency, dgr))
        connection.commit()

    def insert_stock_price(connection, stock_symbol, year, month, adjusted_price, real_price):
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO stock_prices (stock_symbol, year, month, adjusted_price, real_price)
                VALUES (%s, %s, %s, %s, %s);
            """, (stock_symbol, year, month, adjusted_price, real_price))
        connection.commit()

    def insert_stock_info_bulk(connection, stock_info_list):
        with connection.cursor() as cursor:
            query = """
                INSERT INTO stock_list (stock_symbol, stock_name, dividend_yield, pe_ratio, dividend_frequency, dgr)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                dividend_yield = VALUES(dividend_yield),
                pe_ratio = VALUES(pe_ratio),
                dividend_frequency = VALUES(dividend_frequency),
                dgr = VALUES(dgr);
            """
            cursor.executemany(query, stock_info_list)
        connection.commit()

    def insert_stock_price_bulk(connection, stock_price_list):
        with connection.cursor() as cursor:
            query = """
                INSERT INTO stock_prices (stock_symbol, year, month, adjusted_price, real_price)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.executemany(query, stock_price_list)
        connection.commit()

if __name__ == '__main__':
    database_controller = DatabaseController()

    # 連接到 MySQL 伺服器
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',  # 使用 'password' 而不是 'passwd'
        database='stocks_db',
        charset='utf8mb4'  # 使用 utf8mb4 支援更多的字符
    )

    try:
        # 創建資料庫和表格
        database_controller.create_database_and_tables(connection)
        
        # 批量插入股票資訊
        stock_info_list = [
            ['AAPL', 'Apple Inc.', 0.00, 32.22, 4, 5.00],
            ['TSLA', 'Tesla Inc.', 0.00, 60.12, 0, 0.00]
        ]
        database_controller.insert_stock_info_bulk(connection, stock_info_list)
        
        # 批量插入股票價格
        stock_price_list = [
            ['AAPL', 2024, 'June', 214.24, 214.24],
            ['AAPL', 2024, 'May', 192.25, 192.25],
            ['AAPL', 2024, 'April', 170.10, 170.33],
            ['TSLA', 2024, 'June', 123.12, 123.12],
            ['TSLA', 2024, 'May', 115.00, 115.00],
            ['TSLA', 2024, 'April', 110.50, 110.75]
        ]
        database_controller.insert_stock_price_bulk(connection, stock_price_list)
    finally:
        connection.close()
