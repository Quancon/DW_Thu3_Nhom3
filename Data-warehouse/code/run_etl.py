from DataExtractor import DataExtractor
from DataTransformer import DataTransformer
from mart_etl import MartETL
import pyodbc
import json
import os
from datetime import datetime
import pandas as pd
import schedule
import time
import sys

class ETLRunner:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
            
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, 'data')
        
        # Initialize connections
        self.control_conn_str = self.create_connection_string('control_db')
        self.staging_conn_str = self.create_connection_string('staging_db')
        self.warehouse_conn_str = self.create_connection_string('warehouse_db')
        
        # Initialize components
        self.extractor = DataExtractor(output_dir=self.data_dir)
        self.transformer = DataTransformer()
        self.mart_etl = MartETL(self.warehouse_conn_str)

    def create_connection_string(self, db_name):
        db_config = self.config['database']
        return (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_name};"
            "Trusted_Connection=yes;"
            "Connection Timeout=30;"
        )

    def start_job(self, job_name):
        """Record job start in control database"""
        conn = pyodbc.connect(self.control_conn_str)
        cursor = conn.cursor()
        
        cursor.execute("SELECT job_id FROM ETL_Jobs WHERE job_name = ?", job_name)
        job_id = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO Job_Status (job_id, status)
            VALUES (?, 'RUNNING')
        """, job_id)
        
        conn.commit()
        status_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
        
        # Thêm log khi bắt đầu job
        cursor.execute("""
            INSERT INTO Logs (job_id, status_id, message, level)
            VALUES (?, ?, ?, ?)
        """, job_id, status_id, f"Starting job: {job_name}", "INFO")
        conn.commit()
        
        conn.close()
        return job_id, status_id

    def end_job(self, job_id, status_id, success, records=0, error_message=None):
        """Record job completion in control database"""
        conn = pyodbc.connect(self.control_conn_str)
        cursor = conn.cursor()
        
        # Get job name for logging
        cursor.execute("SELECT job_name FROM ETL_Jobs WHERE job_id = ?", job_id)
        job_name = cursor.fetchone()[0]
        
        # Update status
        cursor.execute("""
            UPDATE Job_Status 
            SET status = ?, end_time = GETDATE(), 
                records_processed = ?, error_message = ?
            WHERE status_id = ?
        """, 'SUCCESS' if success else 'FAILED', records, error_message, status_id)
        
        # Add completion log
        log_message = f"Job completed: {job_name}" if success else f"Job failed: {job_name} - {error_message}"
        log_level = "INFO" if success else "ERROR"
        
        cursor.execute("""
            INSERT INTO Logs (job_id, status_id, message, level)
            VALUES (?, ?, ?, ?)
        """, job_id, status_id, log_message, log_level)
        
        # Add notification if configured
        cursor.execute("""
            INSERT INTO Job_Notifications (job_id, status_id, notification_type, recipient, message)
            SELECT 
                ?, ?, nc.notification_type, nc.email_recipient,
                ? + CASE WHEN ? = 1 THEN ' - Success' ELSE ' - Failed: ' + ISNULL(?, 'Unknown error') END
            FROM Notification_Config nc
            WHERE nc.job_id = ?
                AND ((? = 1 AND nc.notify_on_success = 1) 
                    OR (? = 0 AND nc.notify_on_failure = 1))
        """, job_id, status_id, f"Job {job_name}", success, error_message, job_id, success, success)
        
        conn.commit()
        conn.close()

    def log_message(self, job_id, status_id, message, level="INFO"):
        """Add a log entry"""
        try:
            conn = pyodbc.connect(self.control_conn_str)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO Logs (job_id, status_id, message, level)
                VALUES (?, ?, ?, ?)
            """, job_id, status_id, message, level)
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error logging message: {str(e)}")

    def load_staging_data(self, json_files):
        """Load data from staging JSON files into staging database"""
        all_data = []
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_data.extend(data)

        if not all_data:
            raise ValueError("No data found in staging files")

        # Convert to DataFrame
        df = pd.DataFrame(all_data)

        # Connect to staging database and load data
        conn = pyodbc.connect(self.staging_conn_str)
        cursor = conn.cursor()

        # Clear existing data
        cursor.execute("TRUNCATE TABLE GoldPrices")

        # Insert new data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
                VALUES (?, ?, ?, ?)
            """, row['GoldType'], row['BuyPrice'], row['SellPrice'], 
                pd.to_datetime(row['UpdateTime']).strftime('%Y-%m-%d %H:%M:%S'))

        conn.commit()
        conn.close()
        return len(df)

    def run_extraction(self):
        """Run all extraction jobs"""
        staging_files = []

        # Extract from PNJ website
        job_id, status_id = self.start_job('extract_pnj')
        try:
            self.log_message(job_id, status_id, "Starting PNJ web extraction")
            json_file = self.extractor.extract_from_pnj()
            staging_files.append(json_file)
            self.log_message(job_id, status_id, f"PNJ extraction completed, file saved: {json_file}")
            self.end_job(job_id, status_id, True)
        except Exception as e:
            self.log_message(job_id, status_id, f"PNJ extraction failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

        # Extract from CSV
        job_id, status_id = self.start_job('extract_csv')
        try:
            csv_file = os.path.join(self.data_dir, "gold_price.csv")
            self.log_message(job_id, status_id, f"Starting CSV extraction from: {csv_file}")
            if os.path.exists(csv_file):
                json_file = self.extractor.extract_from_csv(csv_file)
                staging_files.append(json_file)
                self.log_message(job_id, status_id, f"CSV extraction completed, file saved: {json_file}")
                self.end_job(job_id, status_id, True)
        except Exception as e:
            self.log_message(job_id, status_id, f"CSV extraction failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

        # Load staging data into database
        job_id, status_id = self.start_job('load_staging')
        try:
            self.log_message(job_id, status_id, "Starting staging data load")
            records = self.load_staging_data(staging_files)
            self.log_message(job_id, status_id, f"Loaded {records} records to staging")
            self.end_job(job_id, status_id, True, records=records)
        except Exception as e:
            self.log_message(job_id, status_id, f"Staging load failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_transformation(self):
        """Run transformation job"""
        job_id, status_id = self.start_job('transform_gold_data')
        try:
            self.log_message(job_id, status_id, "Starting data transformation")
            # Get data from staging
            conn = pyodbc.connect(self.staging_conn_str)
            df = pd.read_sql("SELECT * FROM GoldPrices", conn)
            conn.close()

            # Transform data
            transformed_data = self.transformer.transform_data(df)
            self.log_message(job_id, status_id, f"Transformation completed, {len(df)} records processed")
            self.end_job(job_id, status_id, True, len(df))
            return transformed_data
        except Exception as e:
            self.log_message(job_id, status_id, f"Transformation failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_warehouse_load(self, transformed_data):
        """Run warehouse loading job"""
        job_id, status_id = self.start_job('load_warehouse')
        try:
            self.log_message(job_id, status_id, "Starting warehouse load")
            conn = pyodbc.connect(self.warehouse_conn_str)
            cursor = conn.cursor()
            
            # Load dimensions
            date_dim = transformed_data['date_dim']
            gold_type_dim = transformed_data['gold_type_dim']
            fact_table = transformed_data['fact_table']
            
            # Load date dimension
            self.log_message(job_id, status_id, "Loading date dimension")
            for _, row in date_dim.iterrows():
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey = ?)
                    INSERT INTO DimDate (DateKey, Date, Year, Month, Day, Quarter)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, row['DateKey'], row['DateKey'], row['Date'], 
                    row['Year'], row['Month'], row['Day'], row['Quarter'])

            # Load gold type dimension
            self.log_message(job_id, status_id, "Loading gold type dimension")
            for _, row in gold_type_dim.iterrows():
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DimGoldType WHERE GoldType = ?)
                    INSERT INTO DimGoldType (GoldType, Created_at)
                    VALUES (?, ?)
                """, row['GoldType'], row['GoldType'], row['Created_at'])
                
                # Get the actual GoldTypeKey
                cursor.execute("SELECT GoldTypeKey FROM DimGoldType WHERE GoldType = ?", row['GoldType'])
                actual_key = cursor.fetchone()[0]
                # Update the key in our DataFrame for fact table loading
                fact_table.loc[fact_table['GoldTypeKey'] == row['GoldTypeKey'], 'GoldTypeKey'] = actual_key

            # Load fact table
            self.log_message(job_id, status_id, "Loading fact table")
            for _, row in fact_table.iterrows():
                cursor.execute("""
                    INSERT INTO FactGoldPrices 
                    (GoldTypeKey, DateKey, BuyPrice, SellPrice, PriceDifference, PriceDifferencePercentage)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, row['GoldTypeKey'], row['DateKey'], row['BuyPrice'], 
                    row['SellPrice'], row['PriceDifference'], row['PriceDifferencePercentage'])
            
            conn.commit()
            self.log_message(job_id, status_id, f"Warehouse load completed, {len(fact_table)} records loaded")
            self.end_job(job_id, status_id, True, len(fact_table))
        except Exception as e:
            if conn:
                conn.rollback()
            self.log_message(job_id, status_id, f"Warehouse load failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise
        finally:
            if conn:
                conn.close()

    def run_mart_creation(self):
        """Run data mart creation jobs"""
        # Create daily mart
        job_id, status_id = self.start_job('create_daily_mart')
        try:
            self.log_message(job_id, status_id, "Starting daily mart creation")
            success, message = self.mart_etl.create_daily_mart_sp()
            if success:
                self.log_message(job_id, status_id, "Daily mart created successfully")
            self.end_job(job_id, status_id, success, error_message=None if success else message)
        except Exception as e:
            self.log_message(job_id, status_id, f"Daily mart creation failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

        # Create monthly mart
        job_id, status_id = self.start_job('create_monthly_mart')
        try:
            self.log_message(job_id, status_id, "Starting monthly mart creation")
            success, message = self.mart_etl.create_monthly_mart_sp()
            if success:
                self.log_message(job_id, status_id, "Monthly mart created successfully")
            self.end_job(job_id, status_id, success, error_message=None if success else message)
        except Exception as e:
            self.log_message(job_id, status_id, f"Monthly mart creation failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_full_etl(self):
        """Run the complete ETL process"""
        try:
            print("Starting ETL process...")
            
            # Run extraction
            print("Running extraction...")
            self.run_extraction()
            
            # Run transformation
            print("Running transformation...")
            transformed_data = self.run_transformation()
            
            # Load to warehouse
            print("Loading to warehouse...")
            self.run_warehouse_load(transformed_data)
            
            # Create marts
            print("Creating data marts...")
            self.run_mart_creation()
            
            print("ETL process completed successfully!")
            
        except Exception as e:
            print(f"ETL process failed: {str(e)}")
            raise

    def schedule_jobs(self):
        """Schedule jobs based on configuration in control_db"""
        try:
            print("Fetching job schedules from control_db...")
            conn = pyodbc.connect(self.control_conn_str)
            cursor = conn.cursor()
            
            # Get all active schedules
            cursor.execute("""
                SELECT j.job_name, s.schedule_type, s.schedule_time
                FROM Job_Schedule s
                JOIN ETL_Jobs j ON s.job_id = j.job_id
                WHERE s.is_active = 1 AND j.is_active = 1
            """)
            schedules = cursor.fetchall()
            print(f"Found {len(schedules)} active job schedules")
            
            for job_name, schedule_type, schedule_time in schedules:
                print(f"Scheduling {job_name} to run {schedule_type} at {schedule_time}")
                if schedule_type == 'DAILY':
                    # Schedule daily job
                    schedule.every().day.at(schedule_time.strftime('%H:%M')).do(self.run_single_job, job_name)
                elif schedule_type == 'WEEKLY':
                    # Schedule weekly job (runs on Monday)
                    schedule.every().monday.at(schedule_time.strftime('%H:%M')).do(self.run_single_job, job_name)
                elif schedule_type == 'MONTHLY':
                    # Schedule monthly job (runs on first day of month)
                    schedule.every().day.at(schedule_time.strftime('%H:%M')).do(
                        self.run_monthly_job, job_name
                    )
            
            conn.close()
            return True
        except Exception as e:
            print(f"Error scheduling jobs: {str(e)}")
            return False

    def run_monthly_job(self, job_name):
        """Wrapper to run monthly jobs only on first day of month"""
        if datetime.now().day == 1:
            self.run_single_job(job_name)

    def run_single_job(self, job_name):
        """Run a single ETL job"""
        try:
            if job_name == 'extract_pnj':
                self.run_pnj_extraction()
            elif job_name == 'extract_csv':
                self.run_csv_extraction()
            elif job_name == 'load_staging':
                self.run_staging_load()
            elif job_name == 'transform_gold_data':
                transformed_data = self.run_transformation()
                return transformed_data
            elif job_name == 'load_warehouse':
                self.run_warehouse_load(self.last_transformed_data)
            elif job_name == 'create_daily_mart':
                self.run_daily_mart()
            elif job_name == 'create_monthly_mart':
                self.run_monthly_mart()
            return True
        except Exception as e:
            print(f"Error running job {job_name}: {str(e)}")
            return False

    def run_scheduler(self):
        """Run the scheduler"""
        print("Starting ETL scheduler...")
        if not self.schedule_jobs():
            print("Failed to schedule jobs. Exiting...")
            return
        
        print("Scheduler is running. Press Ctrl+C to stop.")
        print("Scheduled jobs:")
        for job in schedule.get_jobs():
            print(f"- {job}")
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                print("\nScheduler stopped by user")
                break
            except Exception as e:
                print(f"Scheduler error: {str(e)}")
                time.sleep(300)  # Wait 5 minutes on error

    def run_pnj_extraction(self):
        """Run PNJ extraction job"""
        job_id, status_id = self.start_job('extract_pnj')
        try:
            self.log_message(job_id, status_id, "Starting PNJ web extraction")
            json_file = self.extractor.extract_from_pnj()
            self.log_message(job_id, status_id, f"PNJ extraction completed, file saved: {json_file}")
            self.end_job(job_id, status_id, True)
            return json_file
        except Exception as e:
            self.log_message(job_id, status_id, f"PNJ extraction failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_csv_extraction(self):
        """Run CSV extraction job"""
        job_id, status_id = self.start_job('extract_csv')
        try:
            csv_file = os.path.join(self.data_dir, "gold_price.csv")
            self.log_message(job_id, status_id, f"Starting CSV extraction from: {csv_file}")
            json_file = self.extractor.extract_from_csv(csv_file)
            self.log_message(job_id, status_id, f"CSV extraction completed, file saved: {json_file}")
            self.end_job(job_id, status_id, True)
            return json_file
        except Exception as e:
            self.log_message(job_id, status_id, f"CSV extraction failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_staging_load(self):
        """Run staging load job"""
        job_id, status_id = self.start_job('load_staging')
        try:
            self.log_message(job_id, status_id, "Starting staging data load")
            # Get latest staging files
            staging_dir = os.path.join(self.data_dir, 'staging')
            json_files = [
                os.path.join(staging_dir, f) 
                for f in os.listdir(staging_dir) 
                if f.endswith('.json')
            ]
            records = self.load_staging_data(json_files)
            self.log_message(job_id, status_id, f"Loaded {records} records to staging")
            self.end_job(job_id, status_id, True, records=records)
        except Exception as e:
            self.log_message(job_id, status_id, f"Staging load failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_daily_mart(self):
        """Run daily mart creation"""
        job_id, status_id = self.start_job('create_daily_mart')
        try:
            self.log_message(job_id, status_id, "Starting daily mart creation")
            success, message = self.mart_etl.create_daily_mart_sp()
            if success:
                self.log_message(job_id, status_id, "Daily mart created successfully")
            self.end_job(job_id, status_id, success, error_message=None if success else message)
        except Exception as e:
            self.log_message(job_id, status_id, f"Daily mart creation failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def run_monthly_mart(self):
        """Run monthly mart creation"""
        job_id, status_id = self.start_job('create_monthly_mart')
        try:
            self.log_message(job_id, status_id, "Starting monthly mart creation")
            success, message = self.mart_etl.create_monthly_mart_sp()
            if success:
                self.log_message(job_id, status_id, "Monthly mart created successfully")
            self.end_job(job_id, status_id, success, error_message=None if success else message)
        except Exception as e:
            self.log_message(job_id, status_id, f"Monthly mart creation failed: {str(e)}", "ERROR")
            self.end_job(job_id, status_id, False, error_message=str(e))
            raise

    def load_to_warehouse(self, staging_data):
        """Load data from staging to warehouse"""
        print("Loading to warehouse...")
        try:
            # Kết nối đến warehouse database
            warehouse_conn = create_warehouse_connection()
            cursor = warehouse_conn.cursor()
            
            # Đếm số bản ghi trước khi insert
            cursor.execute("SELECT COUNT(*) FROM GoldPrices")
            before_count = cursor.fetchone()[0]
            print(f"Records before loading: {before_count}")

            # Insert từng bản ghi
            for row in staging_data:
                cursor.execute("""
                    INSERT INTO GoldPrices (GoldType, BuyPrice, SellPrice, UpdateTime)
                    VALUES (?, ?, ?, ?)
                """, (
                    row['GoldType'],
                    row['BuyPrice'],
                    row['SellPrice'],
                    pd.to_datetime(row['UpdateTime']).strftime('%Y-%m-%d %H:%M:%S')
                ))
                
            # Commit các thay đổi
            warehouse_conn.commit()
            
            # Đếm số bản ghi sau khi insert
            cursor.execute("SELECT COUNT(*) FROM GoldPrices")
            after_count = cursor.fetchone()[0]
            print(f"Records after loading: {after_count}")
            print(f"New records added: {after_count - before_count}")
            
            # Hiển thị 5 bản ghi mới nhất
            cursor.execute("""
                SELECT TOP 5 GoldType, BuyPrice, SellPrice, UpdateTime 
                FROM GoldPrices 
                ORDER BY UpdateTime DESC
            """)
            print("\nLatest records:")
            for row in cursor.fetchall():
                print(row)
                
            warehouse_conn.close()
            return True
        except Exception as e:
            print(f"Error loading to warehouse: {str(e)}")
            return False

if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
    runner = ETLRunner(config_path)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--schedule':
        # Run in scheduler mode
        runner.run_scheduler()
    else:
        # Run immediately
        runner.run_full_etl() 