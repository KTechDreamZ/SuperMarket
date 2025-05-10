import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

CSV_FILE_PATH = "/opt/airflow/data/sales_data.csv"
POSTGRES_CONN_ID = "project_connection"
TABLE_NAME = "sales_extraction_dag"

def create_table():
    """Creates the sales table if it doesn't exist."""
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            "Transaction ID" TEXT PRIMARY KEY,
            "Total Sales Amount" TEXT,
            "Units Sold" TEXT,
            "Product ID" TEXT,
            "Product Price" TEXT,
            "Product Name" TEXT,
            "Product Category" TEXT,
            "GST Total Amount" TEXT,
            "Payment Method" TEXT,
            "Transaction Date" TEXT,
            "Sales Type" TEXT,
            "Wholesaler ID" TEXT,
            "Wholesaler Name" TEXT,
            "Store ID" TEXT,
            "State" TEXT,
            "City" TEXT,
            "Sales Rep ID" TEXT,
            "Sales Rep Name" TEXT,
            "File Name" TEXT,
            "Load Date" TEXT
        )
    """
    logging.info("Creating table if it does not exist...")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_hook.run(create_table_query)

def extract_and_load():
    """Extracts data from CSV and loads into PostgreSQL with all values as strings."""
    try:
        create_table()
        logging.info("Reading CSV file...")

        df = pd.read_csv(CSV_FILE_PATH)
        df.columns = df.columns.str.strip()

        required_columns = [
            "Transaction ID", "Total Sales Amount", "Units Sold", "Product ID", "Product Price",
            "Product Name", "Product Category", "GST Total Amount", "Payment Method",
            "Transaction Date", "Sales Type", "Wholesaler ID", "Wholesaler Name",
            "Store ID", "State", "City", "Sales Rep ID", "Sales Rep Name"
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        df = df.astype(str)
        df["File Name"] = "sales_ext"
        df["Load Date"] = datetime.today().strftime('%Y-%m-%d')

        required_columns.extend(["File Name", "Load Date"])
        data = [tuple(row) for row in df[required_columns].values]

        logging.info("Connecting to PostgreSQL...")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                {', '.join(f'"{col}"' for col in required_columns)}
            ) VALUES ({', '.join(['%s'] * len(required_columns))})
            ON CONFLICT ("Transaction ID") DO NOTHING
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data)
                conn.commit()
                logging.info("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        logging.error(f"Error in extract_and_load: {e}")
        raise
