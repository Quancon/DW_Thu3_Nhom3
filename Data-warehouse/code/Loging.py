import csv
import os
import json
import pyodbc
from datetime import datetime

def load_config(file_path='config.json'):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
# Ghi log vào cơ sở dữ liệu
def log_to_database(conn, name, action, source, level, file_path):
    sql_insert_log = """
    INSERT INTO logs (name, action, source, level, file_path)
    VALUES (?, ?, ?, ?, ?);
    """
    conn.execute(sql_insert_log, (name, action, source, level, file_path))
    conn.commit()

# Ghi log ra file CSV
def log_to_csv(log_data, csv_file_path):
    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(log_data)

# Hàm chính để ghi log
def create_log(name, action, source, level, csv_file_path, connection_string):
    # Kết nối đến cơ sở dữ liệu
    conn = pyodbc.connect(connection_string)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    file_path = os.path.abspath(__file__)

    # Ghi log vào cơ sở dữ liệu
    log_to_database(conn, name, action, source, level, file_path)

    # Dữ liệu log để ghi vào CSV
    log_data = [None, name, action, source, timestamp, level, file_path]  # ID sẽ tự động tăng
    log_to_csv(log_data, csv_file_path)

    # Đóng kết nối
    conn.close()

if __name__ == "__main__":
    config = load_config()
    db_config = config['database']
    connection_string = (
        f"DRIVER={{{db_config['driver']}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
        #f"UID={db_config['username']};"
        #f"PWD={db_config['password']}"
    )
    
    csv_file_path = 'logs.csv'
    create_log("User1", "Run Code", "Script", "INFO", csv_file_path, connection_string)
    