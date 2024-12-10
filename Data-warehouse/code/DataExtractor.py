import pandas as pd
import json
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
import os
from datetime import datetime
from Loging import create_log

class DataExtractor:
    def __init__(self, output_dir='data'):
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def extract_from_pnj(self, connection_string=None):
        """Extract dữ liệu từ website PNJ blog
        Quy trình ETL:
        1. Extract: Crawl dữ liệu từ web
        2. Transform: Chuẩn hóa dữ liệu theo format chung
        3. Load: Lưu vào staging area (JSON)
        """
        try:
            # 1. Extract - Crawl dữ liệu từ web
            print("Starting web crawling process...")
            
            # 1.1. Khởi tạo webdriver
            options = webdriver.EdgeOptions()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            print("Initializing web driver...")
            driver = webdriver.Edge(options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # 1.2. Truy cập trang web
            url = 'https://www.pnj.com.vn/blog/gia-vang/'
            print(f"Accessing URL: {url}")
            driver.get(url)
            
            # 1.3. Đợi và lấy dữ liệu
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            wait = WebDriverWait(driver, 20)
            
            print("Waiting for price table...")
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#content-price')))
            
            import time
            time.sleep(2)
            
            # 2. Transform - Chuẩn hóa dữ liệu
            print("Transforming data...")
            rows = driver.find_elements(By.CSS_SELECTOR, '#content-price tr')
            transformed_data = []

            for row in rows:
                try:
                    # 2.1. Extract row data
                    columns = row.find_elements(By.TAG_NAME, 'td')
                    if len(columns) != 3:
                        continue
                        
                    # 2.2. Clean and validate data
                    gold_type = columns[0].text.strip()
                    if not gold_type:
                        print("Warning: Empty gold type")
                        continue
                        
                    try:
                        buy_price = float(columns[1].text.strip().replace(',', ''))
                        sell_price = float(columns[2].text.strip().replace(',', ''))
                        
                        if buy_price < 0 or sell_price < 0:
                            print(f"Warning: Negative price found - Buy: {buy_price}, Sell: {sell_price}")
                            continue
                    except ValueError:
                        print(f"Warning: Invalid price format for {gold_type}")
                        continue
                    
                    # 2.3. Create standardized record
                    record = {
                        "GoldType": gold_type,
                        "BuyPrice": buy_price,
                        "SellPrice": sell_price,
                        "UpdateTime": datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                    }
                    transformed_data.append(record)
                except Exception as row_error:
                    print(f"Error processing row: {str(row_error)}")
                    continue
            
            driver.quit()
            print(f"Found {len(transformed_data)} valid gold price entries")

            if not transformed_data:
                raise ValueError("No valid data was extracted from the page")

            # 3. Load - Lưu vào staging area
            # 3.1. Tạo thư mục staging nếu chưa có
            staging_dir = os.path.join(self.output_dir, 'staging')
            if not os.path.exists(staging_dir):
                os.makedirs(staging_dir)
            
            # 3.2. Lưu file JSON vào staging
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            json_filename = f'staging_pnj_{timestamp}.json'
            json_path = os.path.join(staging_dir, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, ensure_ascii=False, indent=4)
            
            print(f"Successfully extracted {len(transformed_data)} records")
            print(f"Staging data saved to: {json_path}")
            
            # 3.3. Log kết quả
            if connection_string:
                create_log("ExtractPNJSuccess", 
                          f"Extracted {len(transformed_data)} records to {json_path}", 
                          "extract_from_pnj", 
                          "INFO", 
                          url, 
                          connection_string)
            
            return json_path
            
        except Exception as e:
            error_msg = f"Error in ETL process: {str(e)}"
            print(error_msg)
            if connection_string:
                create_log("ExtractPNJError", 
                          error_msg, 
                          "extract_from_pnj", 
                          "ERROR", 
                          url, 
                          connection_string)
            raise e

    def extract_from_csv(self, input_file, connection_string=None):
        """Extract dữ liệu từ file CSV về giá vàng
        Quy trình ETL:
        1. Extract: Đọc dữ liệu từ CSV
        2. Transform: Chuẩn hóa dữ liệu theo format chung
        3. Load: Lưu vào staging area (JSON)
        """
        try:
            # 1. Extract - Đọc dữ liệu từ CSV
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            print(f"Reading CSV file: {input_file}")
            df = pd.read_csv(input_file)
            
            # 2. Transform - Chuẩn hóa dữ liệu
            # 2.1. Validate và clean columns
            required_columns = ['type', 'buy', 'sell', 'update']
            df.columns = df.columns.str.strip().str.lower()
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # 2.2. Clean data
            df = df.dropna(subset=required_columns)  # Bỏ dòng thiếu dữ liệu
            
            # 2.3. Transform data
            transformed_data = []
            for _, row in df.iterrows():
                try:
                    # Clean gold type
                    gold_type = str(row['type']).strip()
                    if not gold_type or pd.isna(gold_type):
                        continue
                    
                    # Clean prices
                    try:
                        buy_price = float(str(row['buy']).replace(',', ''))
                        sell_price = float(str(row['sell']).replace(',', ''))
                        if buy_price < 0 or sell_price < 0:
                            print(f"Warning: Negative price found - Buy: {buy_price}, Sell: {sell_price}")
                            continue
                    except ValueError:
                        print(f"Warning: Invalid price format - Buy: {row['buy']}, Sell: {row['sell']}")
                        continue
                    
                    # Clean date
                    try:
                        update_time = pd.to_datetime(row['update'])
                        update_time_str = update_time.strftime('%d/%m/%Y %H:%M:%S')
                    except:
                        print(f"Warning: Invalid date format: {row['update']}")
                        continue
                    
                    # Create standardized record
                    record = {
                        'GoldType': gold_type,
                        'BuyPrice': buy_price,
                        'SellPrice': sell_price,
                        'UpdateTime': update_time_str
                    }
                    transformed_data.append(record)
                except Exception as e:
                    print(f"Warning: Error processing row: {str(e)}")
                    continue
            
            if not transformed_data:
                raise ValueError("No valid data found in CSV file")
            
            # 3. Load - Lưu vào staging area
            # 3.1. Tạo thư mục staging nếu chưa có
            staging_dir = os.path.join(self.output_dir, 'staging')
            if not os.path.exists(staging_dir):
                os.makedirs(staging_dir)
            
            # 3.2. Lưu file JSON vào staging
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            json_filename = f'staging_csv_{timestamp}.json'
            json_path = os.path.join(staging_dir, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, ensure_ascii=False, indent=4)
            
            print(f"Successfully extracted {len(transformed_data)} records")
            print(f"Staging data saved to: {json_path}")
            
            # 3.3. Log kết quả
            if connection_string:
                create_log("ExtractCSVSuccess", 
                          f"Extracted {len(transformed_data)} records to {json_path}", 
                          "extract_from_csv", 
                          "INFO", 
                          input_file, 
                          connection_string)
            
            return json_path
            
        except Exception as e:
            error_msg = f"Error in ETL process: {str(e)}"
            print(error_msg)
            if connection_string:
                create_log("ExtractCSVError", error_msg, "extract_from_csv", "ERROR", input_file, connection_string)
            raise e

    def extract_from_excel(self, input_file, sheet_name=0, connection_string=None):
        """Extract dữ liệu từ file Excel
        Quy trình ETL:
        1. Extract: Đọc dữ liệu từ Excel
        2. Transform: Chuẩn hóa dữ liệu theo format chung
        3. Load: Lưu vào staging area (JSON)
        """
        try:
            # 1. Extract - Đọc dữ liệu từ Excel
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            print(f"Reading Excel file: {input_file}")
            df = pd.read_excel(input_file, sheet_name=sheet_name, engine='openpyxl')
            
            # 2. Transform - Chuẩn hóa dữ liệu
            # 2.1. Validate và clean columns
            required_columns = ['type', 'buy', 'sell', 'update']
            df.columns = df.columns.str.strip().str.lower()
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # 2.2. Clean data
            df = df.dropna(subset=required_columns)  # Bỏ dòng thiếu dữ liệu
            
            # 2.3. Transform data
            transformed_data = []
            for _, row in df.iterrows():
                try:
                    # Clean gold type
                    gold_type = str(row['type']).strip()
                    if not gold_type or pd.isna(gold_type):
                        continue
                    
                    # Clean prices
                    try:
                        buy_price = float(str(row['buy']).replace(',', ''))
                        sell_price = float(str(row['sell']).replace(',', ''))
                        if buy_price < 0 or sell_price < 0:
                            print(f"Warning: Negative price found - Buy: {buy_price}, Sell: {sell_price}")
                            continue
                    except ValueError:
                        print(f"Warning: Invalid price format - Buy: {row['buy']}, Sell: {row['sell']}")
                        continue
                    
                    # Clean date
                    try:
                        update_time = pd.to_datetime(row['update'])
                        update_time_str = update_time.strftime('%d/%m/%Y %H:%M:%S')
                    except:
                        print(f"Warning: Invalid date format: {row['update']}")
                        continue
                    
                    # Create standardized record
                    record = {
                        'GoldType': gold_type,
                        'BuyPrice': buy_price,
                        'SellPrice': sell_price,
                        'UpdateTime': update_time_str
                    }
                    transformed_data.append(record)
                except Exception as e:
                    print(f"Warning: Error processing row: {str(e)}")
                    continue
            
            if not transformed_data:
                raise ValueError("No valid data found in Excel file")
            
            # 3. Load - Lưu vào staging area
            # 3.1. Tạo thư mục staging nếu chưa có
            staging_dir = os.path.join(self.output_dir, 'staging')
            if not os.path.exists(staging_dir):
                os.makedirs(staging_dir)
            
            # 3.2. Lưu file JSON vào staging
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            json_filename = f'staging_excel_{timestamp}.json'
            json_path = os.path.join(staging_dir, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, ensure_ascii=False, indent=4)
            
            print(f"Successfully extracted {len(transformed_data)} records")
            print(f"Staging data saved to: {json_path}")
            
            # 3.3. Log kết quả
            if connection_string:
                create_log("ExtractExcelSuccess", 
                          f"Extracted {len(transformed_data)} records to {json_path}", 
                          "extract_from_excel", 
                          "INFO", 
                          input_file, 
                          connection_string)
            
            return json_path
            
        except Exception as e:
            error_msg = f"Error in ETL process: {str(e)}"
            print(error_msg)
            if connection_string:
                create_log("ExtractExcelError", error_msg, "extract_from_excel", "ERROR", input_file, connection_string)
            raise e
