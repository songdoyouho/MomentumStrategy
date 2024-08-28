import pymysql

def get_first_day_and_yesterday():
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
            WHERE `date` >= '1996-01-01'
            ORDER BY `date` ASC;
            """
            cursor.execute(sql)
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

# # 輸出結果
# import json

# # 將結果轉換為 JSON 格式
# json_data = json.dumps(result, indent=4)

# # 將 JSON 資料寫入文件
# with open('first_open_day_and_previous_python.json', 'w') as json_file:
#     json_file.write(json_data)

# print("JSON 資料已成功輸出到 'first_open_day_and_previous_python.json'")