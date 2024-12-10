import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import json
import os
from sqlalchemy import create_engine, text

class ETLDashboard:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Khởi tạo connection strings
        self.control_engine = self.create_engine('control_db')
        self.staging_engine = self.create_engine('staging_db')
        self.warehouse_engine = self.create_engine('warehouse_db')

    def create_engine(self, db_name):
        db_config = self.config['database']
        conn_str = (
            f"mssql+pyodbc://{db_config['server']}/{db_name}?"
            f"driver={db_config['driver'].replace(' ', '+')}&"
            "trusted_connection=yes&TrustServerCertificate=yes"
        )
        return create_engine(conn_str)

    def get_job_status(self):
        """Lấy trạng thái của các jobs"""
        query = text("""
            SELECT 
                j.job_name,
                s.status,
                s.start_time,
                s.end_time,
                s.records_processed,
                s.error_message
            FROM Job_Status s
            JOIN ETL_Jobs j ON s.job_id = j.job_id
            ORDER BY s.start_time DESC
        """)
        with self.control_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_job_logs(self):
        """Lấy logs của các jobs"""
        query = text("""
            SELECT 
                j.job_name,
                l.message,
                l.level,
                l.created_at
            FROM Logs l
            JOIN ETL_Jobs j ON l.job_id = j.job_id
            ORDER BY l.created_at DESC
        """)
        with self.control_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_gold_prices(self):
        """Lấy dữ liệu giá vàng từ warehouse"""
        query = text("""
            SELECT 
                f.DateKey,
                g.GoldType,
                f.BuyPrice,
                f.SellPrice,
                f.PriceDifference,
                f.PriceDifferencePercentage
            FROM FactGoldPrices f
            JOIN DimGoldType g ON f.GoldTypeKey = g.GoldTypeKey
            ORDER BY f.DateKey DESC
        """)
        with self.warehouse_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_daily_aggregates(self):
        """Lấy dữ liệu tổng hợp theo ngày"""
        query = text("""
            SELECT *
            FROM AggDailyGoldPrices
            ORDER BY DateKey DESC
        """)
        with self.warehouse_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_monthly_aggregates(self):
        """Lấy dữ liệu tổng hợp theo tháng"""
        query = text("""
            SELECT *
            FROM AggMonthlyGoldPrices
            ORDER BY Year DESC, Month DESC
        """)
        with self.warehouse_engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df

def main():
    st.set_page_config(page_title="ETL Dashboard", layout="wide")
    st.title("ETL System Dashboard")

    # Khởi tạo dashboard
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.json')
    dashboard = ETLDashboard(config_path)

    # Tạo tabs
    tab1, tab2, tab3, tab4 = st.tabs(["Job Status", "Logs", "Gold Prices", "Analytics"])

    # Tab 1: Job Status
    with tab1:
        st.header("ETL Job Status")
        try:
            job_status = dashboard.get_job_status()
            if not job_status.empty:
                # Tạo bảng status với màu sắc
                def color_status(val):
                    if val == 'SUCCESS':
                        return 'background-color: #90EE90'
                    elif val == 'FAILED':
                        return 'background-color: #FFB6C1'
                    return ''
                
                styled_status = job_status.style.map(color_status, subset=['status'])
                st.dataframe(styled_status)
            else:
                st.info("No job status data available")
        except Exception as e:
            st.error(f"Error loading job status: {str(e)}")

    # Tab 2: Logs
    with tab2:
        st.header("ETL Logs")
        try:
            logs = dashboard.get_job_logs()
            if not logs.empty:
                # Filter logs by level
                log_level = st.selectbox("Filter by Log Level", ['All'] + list(logs['level'].unique()))
                if log_level != 'All':
                    logs = logs[logs['level'] == log_level]
                
                st.dataframe(logs)
            else:
                st.info("No logs available")
        except Exception as e:
            st.error(f"Error loading logs: {str(e)}")

    # Tab 3: Gold Prices
    with tab3:
        st.header("Gold Prices Data")
        try:
            prices = dashboard.get_gold_prices()
            if not prices.empty:
                # Filter by gold type
                gold_type = st.selectbox("Select Gold Type", ['All'] + list(prices['GoldType'].unique()))
                if gold_type != 'All':
                    prices = prices[prices['GoldType'] == gold_type]
                
                # Create price chart
                fig = px.line(prices, x='DateKey', y=['BuyPrice', 'SellPrice'], 
                             title=f'Gold Prices Over Time - {gold_type}')
                st.plotly_chart(fig)
                
                st.dataframe(prices)
            else:
                st.info("No gold price data available")
        except Exception as e:
            st.error(f"Error loading gold prices: {str(e)}")

    # Tab 4: Analytics
    with tab4:
        st.header("Analytics")
        
        # Daily aggregates
        st.subheader("Daily Aggregates")
        try:
            daily_agg = dashboard.get_daily_aggregates()
            if not daily_agg.empty:
                # Create daily trends chart
                fig = px.line(daily_agg, x='DateKey', 
                             y=['AvgBuyPrice', 'MinBuyPrice', 'MaxBuyPrice'],
                             title='Daily Gold Price Trends')
                st.plotly_chart(fig)
                
                st.dataframe(daily_agg)
            else:
                st.info("No daily aggregate data available")
        except Exception as e:
            st.error(f"Error loading daily aggregates: {str(e)}")
        
        # Monthly aggregates
        st.subheader("Monthly Aggregates")
        try:
            monthly_agg = dashboard.get_monthly_aggregates()
            if not monthly_agg.empty:
                # Create monthly trends chart
                monthly_agg['YearMonth'] = monthly_agg.apply(lambda x: f"{x['Year']}-{x['Month']:02d}", axis=1)
                fig = px.line(monthly_agg, x='YearMonth',
                             y=['AvgBuyPrice', 'MinBuyPrice', 'MaxBuyPrice'],
                             title='Monthly Gold Price Trends')
                st.plotly_chart(fig)
                
                st.dataframe(monthly_agg)
            else:
                st.info("No monthly aggregate data available")
        except Exception as e:
            st.error(f"Error loading monthly aggregates: {str(e)}")

if __name__ == "__main__":
    main() 