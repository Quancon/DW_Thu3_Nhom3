import pyodbc
from datetime import datetime
import pandas as pd
from DataTransformer import DataTransformer

class MartETL:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        
    def create_daily_mart(self):
        """Create daily aggregates from warehouse data"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Clear existing data
            cursor.execute("TRUNCATE TABLE AggDailyGoldPrices")
            
            # Insert daily aggregates
            cursor.execute("""
                INSERT INTO AggDailyGoldPrices (
                    DateKey, 
                    AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
                    AvgSellPrice, MinSellPrice, MaxSellPrice,
                    AvgPriceDifference
                )
                SELECT 
                    CAST(FORMAT(UpdateTime, 'yyyyMMdd') AS INT) AS DateKey,
                    AVG(BuyPrice) AS AvgBuyPrice,
                    MIN(BuyPrice) AS MinBuyPrice,
                    MAX(BuyPrice) AS MaxBuyPrice,
                    AVG(SellPrice) AS AvgSellPrice,
                    MIN(SellPrice) AS MinSellPrice,
                    MAX(SellPrice) AS MaxSellPrice,
                    AVG(SellPrice - BuyPrice) AS AvgPriceDifference
                FROM GoldPrices
                WHERE Is_deleted = 0
                GROUP BY CAST(FORMAT(UpdateTime, 'yyyyMMdd') AS INT)
            """)
            
            conn.commit()
            return True, "Daily mart created successfully"
            
        except Exception as e:
            return False, str(e)
        finally:
            if conn:
                conn.close()
                
    def create_monthly_mart(self):
        """Create monthly aggregates from warehouse data"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            # Clear existing data
            cursor.execute("TRUNCATE TABLE AggMonthlyGoldPrices")
            
            # Insert monthly aggregates
            cursor.execute("""
                INSERT INTO AggMonthlyGoldPrices (
                    Year, Month,
                    AvgBuyPrice, MinBuyPrice, MaxBuyPrice,
                    AvgSellPrice, MinSellPrice, MaxSellPrice,
                    AvgPriceDifference
                )
                SELECT 
                    YEAR(UpdateTime) AS Year,
                    MONTH(UpdateTime) AS Month,
                    AVG(BuyPrice) AS AvgBuyPrice,
                    MIN(BuyPrice) AS MinBuyPrice,
                    MAX(BuyPrice) AS MaxBuyPrice,
                    AVG(SellPrice) AS AvgSellPrice,
                    MIN(SellPrice) AS MinSellPrice,
                    MAX(SellPrice) AS MaxSellPrice,
                    AVG(SellPrice - BuyPrice) AS AvgPriceDifference
                FROM GoldPrices
                WHERE Is_deleted = 0
                GROUP BY YEAR(UpdateTime), MONTH(UpdateTime)
            """)
            
            conn.commit()
            return True, "Monthly mart created successfully"
            
        except Exception as e:
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def create_daily_mart_sp(self):
        """Create daily aggregates using stored procedure"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute("{CALL sp_CreateDailyMart}")
            cursor.commit()
            
            return True, "Daily mart created successfully using SP"
        except Exception as e:
            return False, str(e)
        finally:
            if conn:
                conn.close()

    def create_monthly_mart_sp(self):
        """Create monthly aggregates using stored procedure"""
        try:
            conn = pyodbc.connect(self.connection_string)
            cursor = conn.cursor()
            
            cursor.execute("{CALL sp_CreateMonthlyMart}")
            cursor.commit()
            
            return True, "Monthly mart created successfully using SP"
        except Exception as e:
            return False, str(e)
        finally:
            if conn:
                conn.close() 