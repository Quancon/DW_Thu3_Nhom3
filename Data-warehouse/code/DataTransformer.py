import pandas as pd
from datetime import datetime
import numpy as np


class DataTransformer:
    def __init__(self):
        self.current_timestamp = datetime.now()

    def clean_data(self, df):
        """Clean and standardize data"""
        try:
            # Tạo bản sao của DataFrame để tránh warning
            df = df.copy()
            print(f"Initial UpdateTime type: {df['UpdateTime'].dtype}")
            print(f"UpdateTime sample: {df['UpdateTime'].head()}")

            # Remove duplicates
            df = df.drop_duplicates()

            # Handle null values using loc
            df.loc[:, 'BuyPrice'] = pd.to_numeric(df['BuyPrice'].fillna(0))
            df.loc[:, 'SellPrice'] = pd.to_numeric(df['SellPrice'].fillna(0))

            # Convert UpdateTime to datetime
            try:
                df.loc[:, 'UpdateTime'] = pd.to_datetime(df['UpdateTime'])
            except Exception as e:
                print(f"Error converting UpdateTime: {str(e)}")
                # Try different format
                df.loc[:, 'UpdateTime'] = pd.to_datetime(df['UpdateTime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                
            # Fill any NaT values with current time
            df.loc[df['UpdateTime'].isna(), 'UpdateTime'] = pd.Timestamp.now()
            
            print(f"Final UpdateTime type: {df['UpdateTime'].dtype}")
            print(f"Cleaned UpdateTime sample: {df['UpdateTime'].head()}")

            return df
        except Exception as e:
            print(f"Error in clean_data: {str(e)}")
            raise

    def calculate_derived_fields(self, df):
        # Tạo bản sao của DataFrame
        df = df.copy()

        # Calculate price difference
        df.loc[:, 'PriceDifference'] = df['SellPrice'] - df['BuyPrice']

        # Calculate price difference percentage
        df.loc[:, 'PriceDifferencePercentage'] = (df['PriceDifference'] / df['BuyPrice']) * 100

        # Round numerical values to 2 decimal places
        df.loc[:, 'BuyPrice'] = df['BuyPrice'].round(2)
        df.loc[:, 'SellPrice'] = df['SellPrice'].round(2)
        df.loc[:, 'PriceDifference'] = df['PriceDifference'].round(2)
        df.loc[:, 'PriceDifferencePercentage'] = df['PriceDifferencePercentage'].round(2)

        return df

    def create_dimensions(self, df):
        """Create dimension tables"""
        try:
            print(f"Creating dimensions with UpdateTime type: {df['UpdateTime'].dtype}")
            
            # Create Date Dimension
            date_dim = pd.DataFrame({
                'DateKey': pd.to_datetime(df['UpdateTime']).dt.strftime('%Y%m%d'),
                'Date': pd.to_datetime(df['UpdateTime']).dt.date,
                'Year': pd.to_datetime(df['UpdateTime']).dt.year,
                'Month': pd.to_datetime(df['UpdateTime']).dt.month,
                'Day': pd.to_datetime(df['UpdateTime']).dt.day,
                'Quarter': pd.to_datetime(df['UpdateTime']).dt.quarter
            }).drop_duplicates()

            # Create Gold Type Dimension
            gold_type_dim = pd.DataFrame({
                'GoldTypeKey': range(1, len(df['GoldType'].unique()) + 1),
                'GoldType': df['GoldType'].unique(),
                'Created_at': self.current_timestamp
            })

            return date_dim, gold_type_dim
        except Exception as e:
            print(f"Error in create_dimensions: {str(e)}")
            print(f"UpdateTime sample: {df['UpdateTime'].head()}")
            raise

    def create_fact_table(self, df, date_dim, gold_type_dim):
        # Merge with dimensions to get keys
        df['DateKey'] = df['UpdateTime'].dt.strftime('%Y%m%d')
        fact_table = df.merge(gold_type_dim, on='GoldType', how='left')

        # Select relevant columns for fact table
        fact_gold_prices = fact_table[[
            'GoldTypeKey', 'DateKey', 'BuyPrice', 'SellPrice',
            'PriceDifference', 'PriceDifferencePercentage'
        ]].copy()

        return fact_gold_prices

    def create_aggregates(self, fact_table, date_dim):
        # Daily aggregates
        daily_agg = fact_table.groupby('DateKey').agg({
            'BuyPrice': ['mean', 'min', 'max'],
            'SellPrice': ['mean', 'min', 'max'],
            'PriceDifference': 'mean'
        }).round(2)

        # Monthly aggregates
        monthly_agg = fact_table.merge(date_dim[['DateKey', 'Year', 'Month']], on='DateKey') \
            .groupby(['Year', 'Month']) \
            .agg({
            'BuyPrice': ['mean', 'min', 'max'],
            'SellPrice': ['mean', 'min', 'max'],
            'PriceDifference': 'mean'
        }).round(2)

        return daily_agg, monthly_agg

    def transform_data(self, data):
        """Transform dữ liệu từ dictionary thành DataFrame và xử lý"""
        try:
            # Convert data to DataFrame if it's not already
            if isinstance(data, pd.DataFrame):
                df = data.copy()
            else:
                df = pd.DataFrame(data)
            
            print(f"Initial data shape: {df.shape}")
            print(f"Initial columns: {df.columns.tolist()}")
            
            # Ensure required columns exist
            required_columns = ['GoldType', 'BuyPrice', 'SellPrice', 'UpdateTime']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"Available columns: {df.columns.tolist()}")
                print(f"Data sample: {df.head().to_dict('records')}")
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Convert numeric columns
            df['BuyPrice'] = pd.to_numeric(df['BuyPrice'].astype(str).str.replace(',', ''), errors='coerce')
            df['SellPrice'] = pd.to_numeric(df['SellPrice'].astype(str).str.replace(',', ''), errors='coerce')
            
            # Convert UpdateTime to datetime
            if df['UpdateTime'].dtype != 'datetime64[ns]':
                # Try multiple datetime formats with dayfirst=True
                for fmt in ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        df['UpdateTime'] = pd.to_datetime(df['UpdateTime'], format=fmt, dayfirst=True)
                        break
                    except:
                        continue
                
                # If all formats fail, try without format but with dayfirst=True
                if df['UpdateTime'].dtype != 'datetime64[ns]':
                    df['UpdateTime'] = pd.to_datetime(df['UpdateTime'], dayfirst=True, errors='coerce')
            
            # Fill missing values
            df['BuyPrice'] = df['BuyPrice'].fillna(0)
            df['SellPrice'] = df['SellPrice'].fillna(0)
            df['UpdateTime'] = df['UpdateTime'].fillna(pd.Timestamp.now())
            
            print(f"After cleaning:")
            print(f"Data shape: {df.shape}")
            print(f"Columns: {df.columns.tolist()}")
            print(f"Data types:\n{df.dtypes}")
            
            # Calculate derived fields
            df = self.calculate_derived_fields(df)
            
            # Create dimensions
            date_dim = pd.DataFrame({
                'DateKey': df['UpdateTime'].dt.strftime('%Y%m%d'),
                'Date': df['UpdateTime'].dt.date,
                'Year': df['UpdateTime'].dt.year,
                'Month': df['UpdateTime'].dt.month,
                'Day': df['UpdateTime'].dt.day,
                'Quarter': df['UpdateTime'].dt.quarter
            }).drop_duplicates()
            
            gold_type_dim = pd.DataFrame({
                'GoldTypeKey': range(1, len(df['GoldType'].unique()) + 1),
                'GoldType': df['GoldType'].unique(),
                'Created_at': self.current_timestamp
            })
            
            # Create fact table
            fact_table = self.create_fact_table(df, date_dim, gold_type_dim)
            
            # Create aggregates
            daily_agg, monthly_agg = self.create_aggregates(fact_table, date_dim)
            
            return {
                'clean_data': df,
                'date_dim': date_dim,
                'gold_type_dim': gold_type_dim,
                'fact_table': fact_table,
                'daily_agg': daily_agg,
                'monthly_agg': monthly_agg
            }
        except Exception as e:
            print(f"Error in transform_data: {str(e)}")
            if isinstance(data, list):
                print(f"Data sample: {data[:1]}")
            elif isinstance(data, pd.DataFrame):
                print(f"DataFrame info:")
                print(data.info())
            else:
                print(f"Data type: {type(data)}")
            raise