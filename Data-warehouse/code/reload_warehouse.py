import pyodbc
import pandas as pd
import json
import os
from datetime import datetime

def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def create_connection(db_name):
    config = load_config()
    db_config = config['database']
    conn_str = (
        f"DRIVER={{{db_config['driver']}}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_name};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def main():
    try:
        # Kết nối đến staging database
        print("Connecting to staging database...")
        staging_conn = create_connection('staging_db')
        staging_cursor = staging_conn.cursor()
        
        # Lấy dữ liệu từ staging
        print("Fetching data from staging...")
        staging_cursor.execute("SELECT * FROM GoldPrices")
        staging_data = staging_cursor.fetchall()
        print(f"Found {len(staging_data)} records in staging")
        
        # Kết nối đến warehouse database
        print("\nConnecting to warehouse database...")
        warehouse_conn = create_connection('warehouse_db')
        warehouse_cursor = warehouse_conn.cursor()
        
        # Xóa dữ liệu cũ trong warehouse
        print("Clearing old data from warehouse...")
        warehouse_cursor.execute("DELETE FROM GoldPrices")
        warehouse_conn.commit()
        
        # Insert dữ liệu mới
        print("Inserting new data to warehouse...")
        for row in staging_data:
            warehouse_cursor.execute("""
                INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
                VALUES (?, ?, ?, ?)
            """, (row.GoldType, row.BuyPrice, row.SellPrice, row.UpdateTime))
        
        warehouse_conn.commit()
        
        # Kiểm tra dữ liệu mới
        print("\nChecking new data in warehouse...")
        warehouse_cursor.execute("SELECT COUNT(*) FROM GoldPrices")
        count = warehouse_cursor.fetchone()[0]
        print(f"Total records in warehouse: {count}")
        
        print("\nLatest 5 records:")
        warehouse_cursor.execute("""
            SELECT GoldType, BuyPrice, SellPrice, UpdateTime 
            FROM GoldPrices 
            ORDER BY UpdateTime DESC
        """)
        for row in warehouse_cursor.fetchmany(5):
            print(row)
        
        # Đóng kết nối
        staging_conn.close()
        warehouse_conn.close()
        
        print("\nData reload completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 