import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.types import String, Integer, Numeric, Date
from datetime import datetime

POSTGRES_CONN_ID = "project_connection"
SOURCE_TABLE = "sales"
TARGET_TABLE = "sales_transformed_dag"

def create_table_if_not_exists():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        "Transaction ID" TEXT PRIMARY KEY,
        "Total Sales Amount" NUMERIC,
        "Units Sold" INTEGER,
        "Product ID" TEXT,
        "Product Price" NUMERIC,
        "Product Name" TEXT,
        "Product Category" TEXT,
        "GST Total Amount" NUMERIC,
        "Payment Method" TEXT,
        "Transaction Date" DATE,
        "Sales Type" TEXT,
        "Wholesaler ID" TEXT,
        "Wholesaler Name" TEXT,
        "Store ID" TEXT,
        "State" TEXT,
        "City" TEXT,
        "Sales Rep ID" TEXT,
        "Sales Rep Name" TEXT,
        "File Name" TEXT,
        "Load Date" DATE
    );
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(create_table_query)
        logging.info("Table checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise

def extract_data():
    try:
        query = f"SELECT * FROM {SOURCE_TABLE};"
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        connection = hook.get_conn()
        df = pd.read_sql(query, connection)
        logging.info("Data extracted successfully.")
        return df
    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

def transform_data(df):
    logging.info("Starting data transformation...")
    try:
        numeric_columns = ['Total Sales Amount', 'Units Sold', 'Product Price', 'GST Total Amount']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        def validate_date(date):
            try:
                return pd.to_datetime(date, errors='coerce')
            except Exception:
                return pd.NaT

        df['Transaction Date'] = df['Transaction Date'].apply(validate_date)
        df.loc[df['Transaction Date'].isna(), 'Transaction Date'] = pd.to_datetime('1900-01-01')
        df['Load Date'] = pd.to_datetime('today').date()

        df['Sales Type'] = df['Sales Type'].replace({
            'Direct': 'Retail',
            'Online': 'E-commerce'
        })

        df['Wholesaler ID'] = df['Wholesaler ID'].replace(['nan', 'None', 'NaN', 'null'], pd.NA)
        df['Wholesaler Name'] = df['Wholesaler Name'].replace(['nan', 'None', 'NaN', 'null'], pd.NA)

        df.fillna({
            'Wholesaler ID': 'Not Available',
            'Wholesaler Name': 'Not Available',
            'Payment Method': 'Unknown',
            'Sales Type': 'Unknown',
        }, inplace=True)

        df['Wholesaler ID'] = df['Wholesaler ID'].astype(str).replace('nan', 'Not Available')
        df['Wholesaler Name'] = df['Wholesaler Name'].astype(str).replace('nan', 'Not Available')

        for col in numeric_columns:
            df[col] = df[col].apply(lambda x: max(x, 0) if pd.notnull(x) else 0)

        logging.info("Data transformation completed.")
        return df
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

def load_data(df):
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        logging.info("Loading data into target table...")

        dtype_mapping = {
            "Transaction ID": String,
            "Total Sales Amount": Numeric,
            "Units Sold": Integer,
            "Product ID": String,
            "Product Price": Numeric,
            "Product Name": String,
            "Product Category": String,
            "GST Total Amount": Numeric,
            "Payment Method": String,
            "Transaction Date": Date,
            "Sales Type": String,
            "Wholesaler ID": String,
            "Wholesaler Name": String,
            "Store ID": String,
            "State": String,
            "City": String,
            "Sales Rep ID": String,
            "Sales Rep Name": String,
            "File Name": String,
            "Load Date": Date,
        }

        df.to_sql(
            TARGET_TABLE,
            engine,
            if_exists='append',
            index=False,
            dtype=dtype_mapping
        )
        logging.info("Data loaded successfully!")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def transform_and_load():
    create_table_if_not_exists()
    df = extract_data()
    transformed_df = transform_data(df)
    load_data(transformed_df)
