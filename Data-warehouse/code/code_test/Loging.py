import json
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
import pyodbc
import os
from datetime import datetime, timedelta
import time 
import pandas as pd
from DataTransformer import DataTransformer
from sqlalchemy import create_engine
import urllib.parse

# Class để tạo object connection và lấy connection_string
class Connection:
    def __init__(self, config_path='D:\DW\Data-warehouse\Data-warehouse\config.json', default_db='staging_db'):
        self.config_path = config_path
        self.config = self.load_config()
        self.default_db = default_db
        self.connection_string = self.create_connection_string(self.default_db)

    def load_config(self):
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def create_connection_string(self, db_name):
        db_config = self.config['database']
        return (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_name};"
            # f"UID={db_config['username']};"
            # f"PWD={db_config['password']};"
            "Trusted_Connection=yes;"
            "Connection Timeout=30;"
        )

    def switch_database(self, new_database):
        self.default_db = new_database
        self.connection_string = self.create_connection_string(new_database)
        print(f"Đã chuyển kết nối sang database: {new_database}")


# Ghi log vào cơ sở dữ liệu
def log_to_database(conn, name, action, source, timestamp, level):
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            sql_insert_log = """
            INSERT INTO Logs (name, action, source, timestamp, level)
            VALUES (?, ?, ?, ?, ?);
            """
            conn.execute(sql_insert_log, (name, action, source, timestamp, level))
            conn.commit()
            break
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"Lỗi khi ghi log vào database sau {max_retries} lần thử: {e}")
                raise
            time.sleep(2)  # Đợi 2 giây trước khi thử lại


# Ghi log ra file CSV
def log_to_csv(log_data, csv_file_path):
    try:
        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(log_data)
    except Exception as e:
        print(f"Lỗi khi ghi log vào CSV: {e}")


# Hàm chính để ghi log
def create_log(name, action, source, level, csv_file_path):
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            connection = Connection(default_db='control_db')
            conn = pyodbc.connect(connection.connection_string)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            file_path = os.path.abspath(__file__)

            log_to_database(conn, name, action, source, timestamp, level)
            log_data = [None, name, action, source, timestamp, level, file_path]
            log_to_csv(log_data, csv_file_path)

            conn.close()
            break
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"Lỗi khi tạo log sau {max_retries} lần thử: {e}")
                raise
            time.sleep(2)

        # Đọc CSV
def read_csv(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            gold_prices = []
            for row in reader:
                if 'type' in row and 'buy' in row and 'sell' in row and 'update' in row:
                    try:
                        # Loại bỏ khoảng trắng thừa và parse thời gian
                        update_str = row['update'].strip()
                        try:
                            update_time = datetime.strptime(update_str, '%d/%m/%Y %H:%M:%S')
                            update_time = update_time.strftime('%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            print(f"Không thể parse thời gian từ CSV. Giá trị: '{update_str}'")
                            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        gold_prices.append({
                            'GoldType': row['type'],
                            'BuyPrice': float(row['buy'].replace(',', '')),
                            'SellPrice': float(row['sell'].replace(',', '')),
                            'UpdateTime': update_time
                        })
                    except ValueError as e:
                        print(f"Lỗi khi chuyển đổi giá trị số trong dòng: {row} - Lỗi: {e}")
            return gold_prices
    except Exception as e:
        print(f"Lỗi khi đọc file CSV: {e}")
        return []

# Crawl dữ liệu từ trang web
def crawl_gold_prices(csv_file_path, connection_string):
    try:
        options = webdriver.EdgeOptions()
        options.add_argument('--headless')
        driver = webdriver.Edge(options=options)

        url = 'https://www.pnj.com.vn/blog/gia-vang/?r=1730738388285'
        driver.get(url)

        rows = driver.find_elements(By.CSS_SELECTOR, '#content-price tr')

        gold_prices = []

        for row in rows:
            columns = row.find_elements(By.TAG_NAME, 'td')
            if len(columns) == 3:
                gold_type = columns[0].text
                buy_price = columns[1].text.replace(',', '')
                sell_price = columns[2].text.replace(',', '')
                gold_prices.append({
                    "GoldType": gold_type,
                    "BuyPrice": float(buy_price),
                    "SellPrice": float(sell_price)
                })

        driver.quit()

        gold_prices_json = json.dumps(gold_prices, ensure_ascii=False, indent=4)

        with open('D:\DW\Data-warehouse\Data-warehouse\data\gold_prices.json', 'w', encoding='utf-8') as f:
            f.write(gold_prices_json)

        print(f"Đã Crawl data về file thành công")
        return gold_prices_json
    except Exception as e:
        create_log("CrawlGoldPricesError", "Error", "crawl_gold_prices", "ERROR", csv_file_path)
        raise e


# Hàm xử lý và load dữ liệu vào cơ sở dữ liệu
def load_data_to_database(data, connection_string, table_name):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Thêm dữ liệu vào cơ sở dữ liệu
        for price in data:
            cursor.execute(f"""
                INSERT INTO {table_name} (GoldType, BuyPrice, SellPrice, UpdateTime)
                VALUES (?, ?, ?, ?)
            """, price['GoldType'], price['BuyPrice'], price['SellPrice'], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Lỗi khi load dữ liệu vào cơ sở dữ liệu: {e}")


# Chuyển đổi dữ liệu từ các nguồn khác nhau và thực hiện ETL
def process_data(file_path, file_type, connection_string, table_name):
    data = []

    # Kiểm tra loại file và xử lý dữ liệu tương ứng
    if file_type == 'csv':
        data = read_csv(file_path)
    if data:
        load_data_to_database(data, connection_string, table_name)
    else:
        print(f"Dữ liệu từ file {file_path} không hợp lệ hoặc không có dữ liệu.")


def insert_gold_prices_to_temp_staging(file_path, connection_string, csv_file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            gold_prices = json.load(file)

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Load dữ liệu vào bảng tạm
        for price in gold_prices:
            cursor.execute("""
                USE staging_db
                INSERT INTO GoldPrices_temp (GoldType, BuyPrice, SellPrice, UpdateTime)
                VALUES (?, ?, ?, ?)
            """, price['GoldType'], price['BuyPrice'], price['SellPrice'], datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        conn.commit()
        conn.close()
    except Exception as e:
        create_log("InsertGoldPricesTempError", "Error", "insert_gold_prices_to_sqlserver", "ERROR", csv_file_path)
        raise e


def compare_and_load_gold_prices(connection_string, csv_file_path):
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            # So sánh dữ liệu trong bảng temp với bảng chính
            cursor.execute("""
                SELECT COUNT(*)
                FROM GoldPrices_temp t
                LEFT JOIN GoldPrices p
                ON t.GoldType = p.GoldType
                AND t.BuyPrice = p.BuyPrice
                AND t.SellPrice = p.SellPrice
                WHERE p.GoldType IS NULL
            """)
            diff_count = cursor.fetchone()[0]

            if diff_count > 0:
                cursor.execute("TRUNCATE TABLE GoldPrices")
                cursor.execute("""
                    INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
                    SELECT GoldType, BuyPrice, SellPrice, UpdateTime FROM GoldPrices_temp
                """)
                conn.commit()

                create_log(
                    "LoadGoldPricesSuccess",
                    "Load Data",
                    "compare_and_load_gold_prices",
                    "INFO",
                    csv_file_path
                )

            cursor.execute("TRUNCATE TABLE GoldPrices_temp")
            conn.commit()
            conn.close()
            break

        except pyodbc.OperationalError as e:
            retry_count += 1
            if retry_count == max_retries:
                create_log(
                    "DatabaseConnectionError",
                    f"Failed after {max_retries} attempts: {str(e)}",
                    "compare_and_load_gold_prices",
                    "ERROR",
                    csv_file_path
                )
                raise
            print(f"Lần thử {retry_count}: Lỗi kết nối, đang thử lại...")
            time.sleep(5)  # Sử dụng time.sleep()


def load_new_data_to_warehouse(staging_connection_string, warehouse_connection_string, csv_file_path):
    """
    Load dữ liệu mới từ staging_db vào warehouse_db.
    """
    try:
        # Kết nối đến staging_db
        staging_conn = pyodbc.connect(staging_connection_string)
        staging_cursor = staging_conn.cursor()

        # Kết nối đến warehouse_db
        warehouse_conn = pyodbc.connect(warehouse_connection_string)
        warehouse_cursor = warehouse_conn.cursor()

        # Truy vấn dữ liệu mới từ staging_db
        query = """
        SELECT s.GoldType, s.BuyPrice, s.SellPrice, s.UpdateTime
        FROM staging_db.dbo.GoldPrices s
        WHERE NOT EXISTS (
            SELECT 1
            FROM warehouse_db.dbo.GoldPrices w
            WHERE s.GoldType = w.GoldType AND s.UpdateTime = w.UpdateTime
        )
        """
        staging_cursor.execute(query)
        new_data = staging_cursor.fetchall()

        # Nếu không có dữ liệu mới thì kết thúc
        if not new_data:
            print("Không có dữ liệu mới để load vào warehouse.")
            staging_conn.close()
            warehouse_conn.close()
            return

        # Thời điểm hiện tại (Created_at và Updated_at)
        current_time = datetime.now()

        # Thêm dữ liệu mới vào warehouse_db
        for row in new_data:
            gold_type = row[0]
            buy_price = row[1]
            sell_price = row[2]
            update_time = row[3]
            expired_date = update_time + timedelta(days=5 * 365)  # Thêm 5 năm

            warehouse_cursor.execute("""
                INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime, Expired_Date, Is_deleted, Created_at, Updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, gold_type, buy_price, sell_price, update_time, expired_date, False, current_time, current_time)

        # Commit thay đổi vào warehouse_db
        warehouse_conn.commit()

        # Log thông tin thành công
        create_log(
            "LoadWarehouseSuccess",
            "Load Data",
            "load_new_data_to_warehouse",
            "INFO",
            csv_file_path
        )

        # Đóng kết nối
        staging_conn.close()
        warehouse_conn.close()

    except Exception as e:
        # Log lỗi nếu xảy ra lỗi
        create_log(
            "LoadWarehouseError",
            "Error",
            "load_new_data_to_warehouse",
            "ERROR",
            csv_file_path
        )
        raise e


def load_transformed_data_to_warehouse(transformed_data, warehouse_connection_string, csv_file_path):
    """
    Load dữ liệu đã được transform vào warehouse
    """
    try:
        conn = pyodbc.connect(warehouse_connection_string)
        cursor = conn.cursor()

        # 1. Load Date Dimension
        for _, row in transformed_data['date_dim'].iterrows():
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = ?)
                INSERT INTO DimDate (DateKey, Date, Year, Month, Day, Quarter)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row['DateKey'], row['DateKey'], row['Date'],
                           row['Year'], row['Month'], row['Day'], row['Quarter'])

        # 2. Load Gold Type Dimension
        for _, row in transformed_data['gold_type_dim'].iterrows():
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM DimGoldType WHERE GoldType = ?)
                INSERT INTO DimGoldType (GoldType)
                VALUES (?)
            """, row['GoldType'], row['GoldType'])

        # 3. Load Fact Table
        for _, row in transformed_data['fact_table'].iterrows():
            cursor.execute("""
                INSERT INTO FactGoldPrices 
                (GoldTypeKey, DateKey, BuyPrice, SellPrice, PriceDifference, PriceDifferencePercentage)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row['GoldTypeKey'], row['DateKey'], row['BuyPrice'],
                           row['SellPrice'], row['PriceDifference'], row['PriceDifferencePercentage'])

        #  4. Load Daily Aggregates
        for date_key, row in transformed_data['daily_agg'].iterrows():
            cursor.execute("""
                      MERGE INTO AggDailyGoldPrices AS target
                      USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source 
                          (DateKey, AvgBuyPrice, MinBuyPrice, MaxBuyPrice, 
                           AvgSellPrice, MinSellPrice, MaxSellPrice, AvgPriceDifference)
                      ON target.DateKey = source.DateKey
                      WHEN MATCHED THEN
                          UPDATE SET 
                              AvgBuyPrice = source.AvgBuyPrice,
                              MinBuyPrice = source.MinBuyPrice,
                              MaxBuyPrice = source.MaxBuyPrice,
                              AvgSellPrice = source.AvgSellPrice,
                              MinSellPrice = source.MinSellPrice,
                              MaxSellPrice = source.MaxSellPrice,
                              AvgPriceDifference = source.AvgPriceDifference
                      WHEN NOT MATCHED THEN
                          INSERT (DateKey, AvgBuyPrice, MinBuyPrice, MaxBuyPrice, 
                                 AvgSellPrice, MinSellPrice, MaxSellPrice, AvgPriceDifference)
                          VALUES (source.DateKey, source.AvgBuyPrice, source.MinBuyPrice, source.MaxBuyPrice,
                                 source.AvgSellPrice, source.MinSellPrice, source.MaxSellPrice, source.AvgPriceDifference);
                  """, date_key, row[('BuyPrice', 'mean')], row[('BuyPrice', 'min')],
                           row[('BuyPrice', 'max')], row[('SellPrice', 'mean')],
                           row[('SellPrice', 'min')], row[('SellPrice', 'max')],
                           row[('PriceDifference', 'mean')])

            # 5. Load Monthly Aggregates
        for (year, month), row in transformed_data['monthly_agg'].iterrows():
            cursor.execute("""
                      MERGE INTO AggMonthlyGoldPrices AS target
                      USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source 
                          (Year, Month, AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
                           AvgSellPrice, MinSellPrice, MaxSellPrice, AvgPriceDifference)
                      ON target.Year = source.Year AND target.Month = source.Month
                      WHEN MATCHED THEN
                          UPDATE SET 
                              AvgBuyPrice = source.AvgBuyPrice,
                              MinBuyPrice = source.MinBuyPrice,
                              MaxBuyPrice = source.MaxBuyPrice,
                              AvgSellPrice = source.AvgSellPrice,
                              MinSellPrice = source.MinSellPrice,
                              MaxSellPrice = source.MaxSellPrice,
                              AvgPriceDifference = source.AvgPriceDifference
                      WHEN NOT MATCHED THEN
                          INSERT (Year, Month, AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
                                 AvgSellPrice, MinSellPrice, MaxSellPrice, AvgPriceDifference)
                          VALUES (source.Year, source.Month, source.AvgBuyPrice, source.MinBuyPrice, 
                                 source.MaxBuyPrice, source.AvgSellPrice, source.MinSellPrice,
                                 source.MaxSellPrice, source.AvgPriceDifference);
                  """, year, month, row[('BuyPrice', 'mean')], row[('BuyPrice', 'min')],
                           row[('BuyPrice', 'max')], row[('SellPrice', 'mean')],
                           row[('SellPrice', 'min')], row[('SellPrice', 'max')],
                           row[('PriceDifference', 'mean')])

        conn.commit()
        create_log(
            "LoadTransformedDataSuccess",
            "Load Data",
            "load_transformed_data_to_warehouse",
            "INFO",
            csv_file_path
        )

    except Exception as e:
        create_log(
            "LoadTransformedDataError",
            "Error",
            "load_transformed_data_to_warehouse",
            "ERROR",
            csv_file_path
        )
        raise e
    finally:
        conn.close()


def get_staging_data(staging_connection_string):
    """
    Lấy dữ liệu từ staging database sử dụng SQLAlchemy
    """
    try:
        # Tạo SQLAlchemy engine từ connection string
        params = urllib.parse.quote_plus(staging_connection_string)
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

        # Thực hiện truy vấn sử dụng SQLAlchemy engine
        query = "SELECT GoldType, BuyPrice, SellPrice, UpdateTime FROM GoldPrices"
        df = pd.read_sql(query, engine)

        return df.to_dict('records')
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu từ staging: {e}")
        raise

if __name__ == "__main__":
    try:
        csv_file_path = 'D:\DW\Data-warehouse\Data-warehouse\logs\logs.csv'
        connection = Connection(default_db='staging_db')
        time.sleep(5)
        # Bước 1: Crawl dữ liệu và lưu vào staging_db
        staging_connection_string = connection.connection_string

        # 1.1: Crawl từ các nguồn khác nhau
        crawl_gold_prices(csv_file_path, staging_connection_string)  # Crawl từ web

        # 1.2: Process dữ liệu từ các nguồn
        insert_gold_prices_to_temp_staging('D:\DW\Data-warehouse\Data-warehouse\data\gold_prices.json',
                                           staging_connection_string, csv_file_path)
        # process_data('D:\DW\Data-warehouse\Data-warehouse\data\gold_price.csv', 'csv', staging_connection_string,
        #              'GoldPrices_temp')

        # 1.3: So sánh và load vào bảng chính của staging
        compare_and_load_gold_prices(staging_connection_string, csv_file_path)

        # Bước 2: Transform dữ liệu
        staging_data = get_staging_data(staging_connection_string)
        transformer = DataTransformer()
        transformed_data = transformer.transform_data(staging_data)

        # Bước 3: Load vào warehouse
        connection.switch_database('warehouse_db')
        warehouse_connection_string = connection.connection_string

        # 3.1: Load dữ liệu đã transform
        load_transformed_data_to_warehouse(transformed_data, warehouse_connection_string, csv_file_path)

        # 3.2: Load dữ liệu theo cách cũ
        load_new_data_to_warehouse(staging_connection_string, warehouse_connection_string, csv_file_path)

        # Bước 4: Ghi log vào control_db
        connection.switch_database('control_db')
        control_connection_string = connection.connection_string
        create_log("UpdateSuccess", "Run Code", "Main", "INFO", csv_file_path)
        print(f"Hoàn thành LoadData")
    except Exception as e:
        # Ghi log lỗi vào control_db
        create_log("MainError", "Error", "Main", "ERROR", csv_file_path)
        raise e