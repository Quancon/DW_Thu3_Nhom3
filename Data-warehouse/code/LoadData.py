import json
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
import pyodbc
import os
import sys
from datetime import datetime, timedelta
import time 
import pandas as pd
from DataTransformer import DataTransformer
from sqlalchemy import create_engine
import urllib.parse
import schedule
import logging
from DataExtractor import DataExtractor

# Class để tạo object connection và lấy connection_string
class Connection:
    def __init__(self, config_path=None, default_db='staging_db'):
        # Xác định đường dẫn tuyệt đối cho config
        if config_path is None:
            self.config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
        else:
            self.config_path = os.path.abspath(config_path)
            
        self.config = self.load_config()
        self.default_db = default_db
        self.connection_string = self.create_connection_string(self.default_db)
        
        # Thiết lập các đường dẫn tuyệt đối
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.logs_dir = os.path.join(self.base_dir, 'logs')
        
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

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
    """Hàm logging tổng hợp"""
    max_retries = 3
    retry_count = 0
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    while retry_count < max_retries:
        try:
            # Ghi log vào database
            connection = Connection(default_db='control_db')
            conn = pyodbc.connect(connection.connection_string)
            cursor = conn.cursor()
            
            sql_insert_log = """
            INSERT INTO Logs (name, action, source, timestamp, level)
            VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(sql_insert_log, (name, action, source, timestamp, level))
            conn.commit()
            
            # Ghi log vào CSV
            log_data = [None, name, action, source, timestamp, level, os.path.abspath(__file__)]
            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(log_data)
            
            conn.close()
            break
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"Error creating log after {max_retries} attempts: {e}")
                raise
            time.sleep(2)


# Đọc CSV
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        gold_prices = []
        
        # Map column names
        column_mapping = {
            'GoldType': ['GoldType', 'type', 'Type'],
            'BuyPrice': ['BuyPrice', 'buy', 'Buy'],
            'SellPrice': ['SellPrice', 'sell', 'Sell'],
            'UpdateTime': ['UpdateTime', 'update', 'Update']
        }
        
        # Find actual column names
        actual_columns = {}
        for target, possible_names in column_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    actual_columns[target] = name
                    break
                    
        if len(actual_columns) != 4:
            raise ValueError(f"Missing required columns. Found: {df.columns}")
            
        # Convert DataFrame to list of dictionaries
        for _, row in df.iterrows():
            try:
                # Parse update time
                update_str = str(row[actual_columns['UpdateTime']]).strip()
                try:
                    update_time = pd.to_datetime(update_str, format='%d/%m/%Y %H:%M:%S', dayfirst=True)
                    update_time = update_time.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    print(f"Cannot parse time from CSV. Value: '{update_str}'")
                    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                gold_prices.append({
                    'GoldType': str(row[actual_columns['GoldType']]),
                    'BuyPrice': float(str(row[actual_columns['BuyPrice']]).replace(',', '')),
                    'SellPrice': float(str(row[actual_columns['SellPrice']]).replace(',', '')),
                    'UpdateTime': update_time
                })
            except ValueError as e:
                print(f"Error converting values in row: {row} - Error: {e}")
        return gold_prices
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def read_excel(file_path):
    try:
        df = pd.read_excel(file_path)
        gold_prices = []
        
        # Clean column names - remove extra spaces and convert to lowercase
        df.columns = df.columns.str.strip().str.lower()
        
        print(f"Excel columns after cleaning: {df.columns.tolist()}")
        
        # Map column names
        column_mapping = {
            'GoldType': ['goldtype', 'type', 'Type', 'gold_type'],
            'BuyPrice': ['buyprice', 'buy', 'Buy', 'buy_price'],
            'SellPrice': ['sellprice', 'sell', 'Sell', 'sell_price'],
            'UpdateTime': ['updatetime', 'update', 'Update', 'update_time']
        }
        
        # Find actual column names
        actual_columns = {}
        for target, possible_names in column_mapping.items():
            for name in possible_names:
                if name.lower() in df.columns:
                    actual_columns[target] = name.lower()
                    break
                    
        if len(actual_columns) != 4:
            raise ValueError(f"Missing required columns. Found: {df.columns}")
            
        # Convert DataFrame to list of dictionaries
        for _, row in df.iterrows():
            try:
                update_str = str(row[actual_columns['UpdateTime']]).strip()
                try:
                    # First try with explicit format and dayfirst=True
                    update_time = pd.to_datetime(update_str, format='%d/%m/%Y %H:%M:%S', dayfirst=True)
                except ValueError:
                    try:
                        # Then try with just dayfirst=True
                        update_time = pd.to_datetime(update_str, dayfirst=True)
                    except ValueError:
                        print(f"Cannot parse time from Excel. Value: '{update_str}'")
                        update_time = pd.Timestamp.now()
                
                update_time = update_time.strftime('%Y-%m-%d %H:%M:%S')

                gold_prices.append({
                    'GoldType': str(row[actual_columns['GoldType']]).strip(),
                    'BuyPrice': float(str(row[actual_columns['BuyPrice']]).replace(',', '')),
                    'SellPrice': float(str(row[actual_columns['SellPrice']]).replace(',', '')),
                    'UpdateTime': update_time
                })
            except ValueError as e:
                print(f"Error converting values in row: {row} - Error: {e}")
        return gold_prices
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return []

def read_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            
        # If data is not a list, try to extract it from a known field
        if isinstance(data, dict):
            if 'DGPlist' in data:
                data = data['DGPlist']
            elif 'IGPList' in data:
                data = data['IGPList']
                
        gold_prices = []
        for item in data:
            try:
                # Handle different possible field names
                gold_type = item.get('GoldType') or item.get('Type') or item.get('Name')
                buy_price = item.get('BuyPrice') or item.get('Buy')
                sell_price = item.get('SellPrice') or item.get('Sell')
                update_time = item.get('UpdateTime') or item.get('DateTime') or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Convert price strings to float if needed
                if isinstance(buy_price, str):
                    buy_price = float(buy_price.replace(',', ''))
                if isinstance(sell_price, str):
                    sell_price = float(sell_price.replace(',', ''))
                    
                # Convert update time to standard format
                if isinstance(update_time, str):
                    try:
                        update_time = pd.to_datetime(update_time, format='%d/%m/%Y %H:%M:%S', dayfirst=True)
                        update_time = update_time.strftime('%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            update_time = pd.to_datetime(update_time, format='%Y-%m-%d %H:%M:%S')
                            update_time = update_time.strftime('%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            print(f"Cannot parse time: '{update_time}'")
                            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                gold_prices.append({
                    'GoldType': str(gold_type),
                    'BuyPrice': float(buy_price),
                    'SellPrice': float(sell_price),
                    'UpdateTime': update_time
                })
            except (ValueError, TypeError) as e:
                print(f"Error converting values in item: {item} - Error: {e}")
                
        return gold_prices
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return []

# Crawl dữ liệu từ trang web
def crawl_gold_prices(csv_file_path, connection_string):
    try:
        options = webdriver.EdgeOptions()
        options.add_argument('--headless')
        driver = webdriver.Edge(options=options)

        url = 'https://www.pnj.com.vn/blog/gia-vang'
        driver.get(url)

        rows = driver.find_elements(By.CSS_SELECTOR, '#content-price tr')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
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
                    "SellPrice": float(sell_price),
                    "UpdateTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

        driver.quit()

        # Sử dụng đường dẫn tuyệt đối
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        output_file = os.path.join(base_dir, 'data', f'web_pnj_blog_{timestamp}.json')
        
        # Đảm bảo thư mục tồn tại
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(gold_prices, f, ensure_ascii=False, indent=4)

        print(f"Crawled data saved to: {output_file}")
        return output_file
    except Exception as e:
        create_log("CrawlGoldPricesError", "Error", "crawl_gold_prices", "ERROR", csv_file_path)
        raise e


# Hàm xử lý và load dữ liệu vào cơ sở dữ liệu
def load_data_to_database(data, connection_string, table_name):
    """Load dữ liệu vào database"""
    conn = None
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Kiểm tra và tạo bảng nếu chưa tồn tại
        if table_name == 'GoldPrices_temp':
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'GoldPrices_temp')
                CREATE TABLE GoldPrices_temp (
                    gold_id INT IDENTITY(1,1) PRIMARY KEY,
                    GoldType NVARCHAR(255) NOT NULL,
                    BuyPrice FLOAT NULL,
                    SellPrice FLOAT NULL,
                    UpdateTime DATETIME NULL
                )
            """)
            conn.commit()

        # Thêm dữ liệu vào cơ sở dữ liệu
        for price in data:
            cursor.execute(f"""
                INSERT INTO {table_name} (GoldType, BuyPrice, SellPrice, UpdateTime)
                VALUES (?, ?, ?, ?)
            """, price['GoldType'], price['BuyPrice'], price['SellPrice'], price['UpdateTime'])

        conn.commit()
        print(f"Loaded {len(data)} records into {table_name}")
    except Exception as e:
        print(f"Error loading data to database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


# Chuyển đổi dữ liệu từ các nguồn khác nhau và thực hiện ETL
def process_data(file_path, file_type, connection_string, table_name):
    # Xóa hàm này vì đã có các hàm riêng biệt cho từng loại file
    pass

def insert_gold_prices_to_temp_staging(file_path, connection_string, csv_file_path):
    # Xóa hàm này vì trùng lặp với load_data_to_database
    pass

def load_new_data_to_warehouse(staging_connection_string, warehouse_connection_string, csv_file_path):
    # Xóa hàm này vì đã được thay thế bởi load_transformed_data_to_warehouse
    pass

def get_staging_data(staging_connection_string):
    # Xóa hàm này vì có thể gộp vào run_warehouse_update_task
    pass

# Tối ưu hàm logging
def create_log(name, action, source, level, csv_file_path):
    """Hàm logging tổng hợp"""
    max_retries = 3
    retry_count = 0
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    while retry_count < max_retries:
        try:
            # Ghi log vào database
            connection = Connection(default_db='control_db')
            conn = pyodbc.connect(connection.connection_string)
            cursor = conn.cursor()
            
            sql_insert_log = """
            INSERT INTO Logs (name, action, source, timestamp, level)
            VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(sql_insert_log, (name, action, source, timestamp, level))
            conn.commit()
            
            # Ghi log vào CSV
            log_data = [None, name, action, source, timestamp, level, os.path.abspath(__file__)]
            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(log_data)
            
            conn.close()
            break
        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                print(f"Error creating log after {max_retries} attempts: {e}")
                raise
            time.sleep(2)

# Cập nhật ETLScheduler
class ETLScheduler:
    def __init__(self, config_path=None):
        print("Initializing ETL Scheduler...")
        try:
            # Xác định đường dẫn tuyệt đối cho config
            if config_path is None:
                self.config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
            else:
                self.config_path = os.path.abspath(config_path)
            
            print(f"Using config file: {self.config_path}")
            self.config = self.load_config()
            
            # Thiết lập các đường dẫn tuyệt đối
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.data_dir = os.path.join(self.base_dir, 'data')
            self.logs_dir = os.path.join(self.base_dir, 'logs')
            
            print(f"Base directory: {self.base_dir}")
            print(f"Data directory: {self.data_dir}")
            print(f"Logs directory: {self.logs_dir}")
            
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(self.data_dir, exist_ok=True)
            os.makedirs(self.logs_dir, exist_ok=True)
            
            # Khởi tạo các components
            self.connection = Connection(self.config_path)
            self.extractor = DataExtractor(output_dir=self.data_dir)
            self.transformer = DataTransformer()
            
            # Thiết lập logging
            log_file = os.path.join(self.logs_dir, 'etl_scheduler.log')
            logging.basicConfig(
                filename=log_file,
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            self.logger = logging.getLogger('ETLScheduler')
            print("ETL Scheduler initialized successfully")
            
        except Exception as e:
            print(f"Error initializing ETL Scheduler: {str(e)}")
            raise

    def load_config(self):
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def run_warehouse_update_task(self):
        """Cập nhật warehouse từ staging"""
        try:
            self.logger.info("Starting warehouse update task")
            self.connection.switch_database('staging_db')
            
            # Kiểm tra dữ liệu trong staging
            conn = pyodbc.connect(self.connection.connection_string)
            cursor = conn.cursor()
            
            # Kiểm tra bảng tồn tại
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'GoldPrices')
                CREATE TABLE GoldPrices (
                    gold_id INT IDENTITY(1,1) PRIMARY KEY,
                    GoldType NVARCHAR(255) NOT NULL,
                    BuyPrice FLOAT NULL,
                    SellPrice FLOAT NULL,
                    UpdateTime DATETIME NULL
                )
            """)
            conn.commit()
            
            # Lấy dữ liệu từ staging
            cursor.execute("SELECT GoldType, BuyPrice, SellPrice, UpdateTime FROM GoldPrices")
            rows = cursor.fetchall()
            
            if not rows:
                print("No data in staging database")
                return
            
            # Chuyển đổi dữ liệu sang DataFrame
            data = []
            for row in rows:
                data.append({
                    'GoldType': row[0],
                    'BuyPrice': float(row[1]),
                    'SellPrice': float(row[2]),
                    'UpdateTime': row[3]
                })
            
            # Transform dữ liệu
            transformed_data = self.transformer.transform_data(data)
            
            # Load vào warehouse
            self.connection.switch_database('warehouse_db')
            load_transformed_data_to_warehouse(transformed_data, self.connection.connection_string)
            self.logger.info("Warehouse update completed")
            
        except Exception as e:
            print(f"Error in warehouse update task: {str(e)}")
            self.logger.error(f"Error in warehouse update task: {str(e)}")
            raise

    def run_file_processing_task(self):
        """Chạy task xử lý các file trong thư mục data"""
        try:
            self.logger.info("Starting file processing task")
            
            file_handlers = {
                '.csv': read_csv,
                '.xlsx': read_excel,
                '.json': read_json
            }
            
            # Process all files
            for file in os.listdir(self.data_dir):
                file_ext = os.path.splitext(file)[1].lower()
                if file_ext in file_handlers:
                    self.logger.info(f"Processing {file_ext} file: {file}")
                    file_path = os.path.join(self.data_dir, file)
                    data = file_handlers[file_ext](file_path)
                    if data:
                        load_data_to_database(data, self.connection.connection_string, 'GoldPrices_temp')
            
            self.logger.info("File processing completed")
            
        except Exception as e:
            self.logger.error(f"Error in file processing task: {str(e)}")

    def run_web_crawling_task(self):
        """Chạy task crawl dữ liệu từ web"""
        print("\nStarting web crawling task...")
        try:
            self.logger.info("Starting web crawling task")
            log_file = os.path.join(self.logs_dir, 'logs.csv')
            
            # Đảm bảo file log tồn tại
            if not os.path.exists(log_file):
                with open(log_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['ID', 'Name', 'Action', 'Source', 'Timestamp', 'Level', 'FilePath'])
            
            json_file = crawl_gold_prices(log_file, self.connection.connection_string)
            print(f"Web crawling completed. Data saved to: {json_file}")
            
            # Load dữ liệu vào staging
            print("Loading data to staging database...")
            data = read_json(json_file)
            if data:
                load_data_to_database(data, self.connection.connection_string, 'GoldPrices_temp')
                print("Data loaded to staging database")
            
            # Transform và load vào warehouse
            print("Transforming and loading data to warehouse...")
            transformed_data = self.transformer.transform_data(data)
            self.connection.switch_database('warehouse_db')
            load_transformed_data_to_warehouse(transformed_data, self.connection.connection_string, log_file)
            print("Data loaded to warehouse database")
            
        except Exception as e:
            print(f"Error in web crawling task: {str(e)}")
            self.logger.error(f"Error in web crawling task: {str(e)}")
            raise

    def setup_schedules(self):
        """Thiết lập lịch chạy các task"""
        try:
            print("Setting up schedules...")
            scheduler_config = self.config['etl']['scheduler']
            
            # Web crawling task
            interval = scheduler_config['web_crawling_interval']
            print(f"Setting up web crawling task to run every {interval} seconds")
            schedule.every(interval).seconds.do(self.run_web_crawling_task)
            
            # File processing task
            interval = scheduler_config['file_processing_interval']
            print(f"Setting up file processing task to run every {interval} seconds")
            schedule.every(interval).seconds.do(self.run_file_processing_task)
            
            # Warehouse update task
            interval = scheduler_config['warehouse_update_interval']
            print(f"Setting up warehouse update task to run every {interval} seconds")
            schedule.every(interval).seconds.do(self.run_warehouse_update_task)
            
            # Daily backup
            backup_time = scheduler_config['backup_time']
            print(f"Setting up daily backup task to run at {backup_time}")
            schedule.every().day.at(backup_time).do(self.run_warehouse_update_task)
            
            print("All schedules have been set up")
            self.logger.info("Schedules have been set up")
            
        except Exception as e:
            print(f"Error setting up schedules: {str(e)}")
            self.logger.error(f"Error setting up schedules: {str(e)}")
            raise

    def run(self):
        """Chạy scheduler"""
        try:
            print("Starting scheduler...")
            self.setup_schedules()
            self.logger.info("ETL Scheduler started")
            
            while True:
                try:
                    schedule.run_pending()
                    time.sleep(1)  # Check mỗi giây
                except Exception as e:
                    print(f"Error in scheduler loop: {str(e)}")
                    self.logger.error(f"Error in scheduler loop: {str(e)}")
                    time.sleep(300)  # Nếu có lỗi, đợi 5 phút rồi thử lại
        except Exception as e:
            print(f"Error in scheduler: {str(e)}")
            self.logger.error(f"Error in scheduler: {str(e)}")
            raise

def compare_and_load_gold_prices(connection_string, log_file):
    """So sánh và load dữ liệu từ bảng temp vào bảng chính"""
    max_retries = 3
    retry_count = 0
    conn = None

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
                # Backup bảng cũ trước khi cập nhật
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                cursor.execute(f"""
                    SELECT * INTO GoldPrices_backup_{timestamp}
                    FROM GoldPrices
                """)
                
                # Cập nhật dữ liệu mới
                cursor.execute("TRUNCATE TABLE GoldPrices")
                cursor.execute("""
                    INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
                    SELECT GoldType, BuyPrice, SellPrice, UpdateTime FROM GoldPrices_temp
                """)
                conn.commit()

                create_log(
                    "LoadGoldPricesSuccess",
                    f"Loaded {diff_count} new records",
                    "compare_and_load_gold_prices",
                    "INFO",
                    log_file
                )
                print(f"Loaded {diff_count} new records into GoldPrices")
            else:
                print("No new data to load")

            cursor.execute("TRUNCATE TABLE GoldPrices_temp")
            conn.commit()
            break

        except Exception as e:
            retry_count += 1
            if retry_count == max_retries:
                create_log(
                    "LoadGoldPricesError",
                    str(e),
                    "compare_and_load_gold_prices",
                    "ERROR",
                    log_file
                )
                if conn:
                    conn.rollback()
                raise
            print(f"Retry {retry_count}: Error occurred, retrying...")
            time.sleep(5)
        finally:
            if conn:
                conn.close()

def load_transformed_data_to_warehouse(transformed_data, warehouse_connection_string, log_file):
    """Load dữ liệu đã được transform vào warehouse"""
    conn = None
    try:
        conn = pyodbc.connect(warehouse_connection_string)
        cursor = conn.cursor()

        # 1. Load Date Dimension
        for _, row in transformed_data['date_dim'].iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = ?)
                    INSERT INTO DimDate (DateKey, Date, Year, Month, Day, Quarter)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, row['DateKey'], row['DateKey'], row['Date'],
                            row['Year'], row['Month'], row['Day'], row['Quarter'])
            except Exception as e:
                print(f"Error loading date dimension: {e}")
                continue

        # 2. Load Gold Type Dimension
        for _, row in transformed_data['gold_type_dim'].iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DimGoldType WHERE GoldType = ?)
                    INSERT INTO DimGoldType (GoldType)
                    VALUES (?)
                """, row['GoldType'], row['GoldType'])
            except Exception as e:
                print(f"Error loading gold type dimension: {e}")
                continue

        # 3. Load Fact Table
        for _, row in transformed_data['fact_table'].iterrows():
            try:
                cursor.execute("""
                    INSERT INTO FactGoldPrices 
                    (GoldTypeKey, DateKey, BuyPrice, SellPrice, PriceDifference, PriceDifferencePercentage)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, row['GoldTypeKey'], row['DateKey'], row['BuyPrice'],
                            row['SellPrice'], row['PriceDifference'], row['PriceDifferencePercentage'])
            except Exception as e:
                print(f"Error loading fact table: {e}")
                continue

        # 4. Load Daily Aggregates
        for date_key, row in transformed_data['daily_agg'].iterrows():
            try:
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
                """, date_key, 
                    row[('BuyPrice', 'mean')], 
                    row[('BuyPrice', 'min')],
                    row[('BuyPrice', 'max')], 
                    row[('SellPrice', 'mean')],
                    row[('SellPrice', 'min')], 
                    row[('SellPrice', 'max')],
                    row[('PriceDifference', 'mean')])
            except Exception as e:
                print(f"Error loading daily aggregates: {e}")
                continue

        # 5. Load Monthly Aggregates
        for (year, month), row in transformed_data['monthly_agg'].iterrows():
            try:
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
                """, year, month, 
                    row[('BuyPrice', 'mean')], 
                    row[('BuyPrice', 'min')],
                    row[('BuyPrice', 'max')], 
                    row[('SellPrice', 'mean')],
                    row[('SellPrice', 'min')], 
                    row[('SellPrice', 'max')],
                    row[('PriceDifference', 'mean')])
            except Exception as e:
                print(f"Error loading monthly aggregates: {e}")
                continue

        conn.commit()
        create_log(
            "LoadTransformedDataSuccess",
            "Load Data",
            "load_transformed_data_to_warehouse",
            "INFO",
            log_file
        )
        print("Successfully loaded transformed data to warehouse")

    except Exception as e:
        if conn:
            conn.rollback()
        create_log(
            "LoadTransformedDataError",
            str(e),
            "load_transformed_data_to_warehouse",
            "ERROR",
            log_file
        )
        raise e
    finally:
        if conn:
            conn.close()

def main():
    try:
        print("\n=== Starting ETL Process ===")
        scheduler = ETLScheduler()
        print("\n=== Running Initial Tasks ===")
        
        # Chạy các task ngay lập tức khi khởi động
        scheduler.run_web_crawling_task()
        scheduler.run_file_processing_task()
        scheduler.run_warehouse_update_task()
        
        print("\n=== Initial Tasks Completed ===")
        print("\n=== Starting Scheduler ===")
        print("Press Ctrl+C to stop")
        scheduler.run()
    except Exception as e:
        print(f"\nError in main: {str(e)}")
        raise
    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
        sys.exit(0)

if __name__ == "__main__":
    main()