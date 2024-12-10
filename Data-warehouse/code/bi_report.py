import pandas as pd
import pyodbc
import json
import os
from datetime import datetime
import csv

class BIDataExporter:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Khởi tạo connection string
        self.warehouse_conn_str = self.create_connection_string('warehouse_db')
        
        # Tạo thư mục export nếu chưa tồn tại
        self.export_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bi_exports')
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)

    def create_connection_string(self, db_name):
        db_config = self.config['database']
        return (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_name};"
            "Trusted_Connection=yes;"
        )

    def export_fact_data(self):
        """Export fact table data"""
        print("Exporting fact data...")
        conn = pyodbc.connect(self.warehouse_conn_str)
        
        query = """
            SELECT 
                f.FactID,
                d.Date,
                d.Year,
                d.Month,
                d.Quarter,
                g.GoldType,
                f.BuyPrice,
                f.SellPrice,
                f.PriceDifference,
                f.PriceDifferencePercentage
            FROM FactGoldPrices f
            JOIN DimDate d ON f.DateKey = d.DateKey
            JOIN DimGoldType g ON f.GoldTypeKey = g.GoldTypeKey
            ORDER BY d.Date DESC
        """
        
        df = pd.read_sql(query, conn)
        output_file = os.path.join(self.export_dir, 'fact_gold_prices.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Fact data exported to: {output_file}")
        
        conn.close()
        return output_file

    def export_daily_aggregates(self):
        """Export daily aggregates"""
        print("Exporting daily aggregates...")
        conn = pyodbc.connect(self.warehouse_conn_str)
        
        query = """
            SELECT 
                d.Date,
                d.Year,
                d.Month,
                d.Quarter,
                a.AvgBuyPrice,
                a.MinBuyPrice,
                a.MaxBuyPrice,
                a.AvgSellPrice,
                a.MinSellPrice,
                a.MaxSellPrice,
                a.AvgPriceDifference
            FROM AggDailyGoldPrices a
            JOIN DimDate d ON a.DateKey = d.DateKey
            ORDER BY d.Date DESC
        """
        
        df = pd.read_sql(query, conn)
        output_file = os.path.join(self.export_dir, 'daily_aggregates.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Daily aggregates exported to: {output_file}")
        
        conn.close()
        return output_file

    def export_monthly_aggregates(self):
        """Export monthly aggregates"""
        print("Exporting monthly aggregates...")
        conn = pyodbc.connect(self.warehouse_conn_str)
        
        query = """
            SELECT 
                Year,
                Month,
                AvgBuyPrice,
                MinBuyPrice,
                MaxBuyPrice,
                AvgSellPrice,
                MinSellPrice,
                MaxSellPrice,
                AvgPriceDifference
            FROM AggMonthlyGoldPrices
            ORDER BY Year DESC, Month DESC
        """
        
        df = pd.read_sql(query, conn)
        output_file = os.path.join(self.export_dir, 'monthly_aggregates.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Monthly aggregates exported to: {output_file}")
        
        conn.close()
        return output_file

    def export_gold_type_analysis(self):
        """Export gold type analysis"""
        print("Exporting gold type analysis...")
        conn = pyodbc.connect(self.warehouse_conn_str)
        
        query = """
            WITH GoldTypeStats AS (
                SELECT 
                    g.GoldType,
                    AVG(f.BuyPrice) as AvgBuyPrice,
                    AVG(f.SellPrice) as AvgSellPrice,
                    AVG(f.PriceDifference) as AvgPriceDiff,
                    AVG(f.PriceDifferencePercentage) as AvgPriceDiffPct,
                    COUNT(*) as TotalRecords
                FROM FactGoldPrices f
                JOIN DimGoldType g ON f.GoldTypeKey = g.GoldTypeKey
                GROUP BY g.GoldType
            )
            SELECT 
                GoldType,
                ROUND(AvgBuyPrice, 2) as AvgBuyPrice,
                ROUND(AvgSellPrice, 2) as AvgSellPrice,
                ROUND(AvgPriceDiff, 2) as AvgPriceDiff,
                ROUND(AvgPriceDiffPct, 2) as AvgPriceDiffPct,
                TotalRecords
            FROM GoldTypeStats
            ORDER BY TotalRecords DESC
        """
        
        df = pd.read_sql(query, conn)
        output_file = os.path.join(self.export_dir, 'gold_type_analysis.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"Gold type analysis exported to: {output_file}")
        
        conn.close()
        return output_file

    def export_all_data(self):
        """Export all data for BI analysis"""
        try:
            print("Starting data export for BI analysis...")
            
            # Export all datasets
            fact_file = self.export_fact_data()
            daily_file = self.export_daily_aggregates()
            monthly_file = self.export_monthly_aggregates()
            analysis_file = self.export_gold_type_analysis()
            
            # Create metadata file
            metadata = {
                'export_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'files': {
                    'fact_data': fact_file,
                    'daily_aggregates': daily_file,
                    'monthly_aggregates': monthly_file,
                    'gold_type_analysis': analysis_file
                }
            }
            
            metadata_file = os.path.join(self.export_dir, 'metadata.json')
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=4)
            
            print("\nData export completed successfully!")
            print(f"All files have been exported to: {self.export_dir}")
            
        except Exception as e:
            print(f"Error during data export: {str(e)}")
            raise

def main():
    # Lấy đường dẫn config
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
    
    # Khởi tạo exporter
    exporter = BIDataExporter(config_path)
    
    # Export dữ liệu
    exporter.export_all_data()

if __name__ == "__main__":
    main() 