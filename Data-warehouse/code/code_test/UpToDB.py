import pyodbc
import json
from datetime import datetime

def load_config(file_path='config.json'):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)
    
def insert_data_to_sqlserver(json_file, connection_string):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Insert DGPlist
    for item in data.get('DGPlist', []):
        cursor.execute("""
            INSERT INTO DGPlist (DateTime, Name, NKey, Sell, Buy, UpdateTime)
            VALUES (?, ?, ?, ?, ?, ?)
        """, item['DateTime'], item['Name'], item['NKey'], float(item['Sell'].replace(',', '')), float(item['Buy'].replace(',', '')), update_time)
    
    # Insert IGPList
    for item in data.get('IGPList', []):
        cursor.execute("""
            INSERT INTO IGPList (DateTime, Name, NKey, Sell, Buy, UpdateTime)
            VALUES (?, ?, ?, ?, ?, ?)
        """, item['DateTime'], item['Name'], item['NKey'], float(item['Sell'].replace(',', '')), float(item['Buy'].replace(',', '')), update_time)
    
    # Insert IGPChart
    for item in data.get('IGPChart', []):
        cursor.execute("""
            INSERT INTO IGPChart (Name, NKey, Url, UpdateTime)
            VALUES (?, ?, ?, ?)
        """, item['Name'], item['NKey'], item['Url'], update_time)
    
    # Insert JewelryList
    for item in data.get('JewelryList', []):
        cursor.execute("""
            INSERT INTO JewelryList (DateTime, Name, NKey, Sell, Buy, UpdateTime)
            VALUES (?, ?, ?, ?, ?, ?)
        """, item['DateTime'], item['Name'], item['NKey'], float(item['Sell'].replace(',', '')) if item['Sell'] != '-' else None, float(item['Buy'].replace(',', '')) if item['Buy'] != '-' else None, update_time)
    
    # Insert GPChart
    for item in data.get('GPChart', []):
        cursor.execute("""
            INSERT INTO GPChart (Name, NKey, Url, UpdateTime)
            VALUES (?, ?, ?, ?)
        """, item['Name'], item['NKey'], item['Url'], update_time)
        
    # Insert Source
    if 'Source' in data:
        cursor.execute("""
            INSERT INTO Source (Source)
            VALUES (?)
        """, data['Source'])
    
    conn.commit()
    conn.close()
    
def insert_gold_prices_to_sqlserver(file_path, connection_string):
    
    with open(file_path, 'r', encoding='utf-8') as file:
        gold_prices = json.load(file)
    
    conn = pyodbc.connect(connection_string)

    cursor = conn.cursor()
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for price in gold_prices:
        cursor.execute("""
            INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
            VALUES (?, ?, ?, ?)
        """, price['GoldType'], price['BuyPrice'], price['SellPrice'], update_time)
    
    conn.commit()
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
    # insert_data_to_sqlserver("gold_data.json", connection_string)
    insert_gold_prices_to_sqlserver("gold_prices.json",connection_string)