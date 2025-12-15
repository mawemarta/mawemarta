# ==============================
# IMPORT LIBRARY
# ==============================
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import os

# ==============================
# SQL SERVER CONFIG (SOURCE)
# ==============================
SQL_SERVER = "PSA-MARTHA"
SQL_DB = "AdventureWorksDW"
SQL_DRIVER = "{ODBC Driver 17 for SQL Server}"

# ==============================
# POSTGRES CONFIG (TARGET)
# ==============================
PG_USER = os.environ.get("PGUID")
PG_PASS = os.environ.get("PGPASS")
PG_HOST = "localhost"          # sesuaikan kalau beda
PG_PORT = "5432"
PG_DB = "AdventureWorks"

# ==============================
# EXTRACT FUNCTION
# ==============================
def extract():
    try:
        print("Connecting to SQL Server...")

        src_conn = pyodbc.connect(
            f"DRIVER={SQL_DRIVER};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DB};"
            "Trusted_Connection=yes;"
        )

        print("Connected to SQL Server")

        query_tables = """
            SELECT t.name AS table_name
            FROM sys.tables t
            WHERE t.name IN (
                'DimProduct',
                'DimProductSubcategory',
                'DimProductCategory',
                'DimSalesTerritory',
                'FactInternetSales'
            )
        """

        df_tables = pd.read_sql_query(query_tables, src_conn)

        for tbl in df_tables["table_name"]:
            print(f"\n Extracting table: {tbl}")
            df = pd.read_sql_query(f"SELECT * FROM {tbl}", src_conn)
            print(f" Extracted {len(df)} rows from {tbl}")
            load(df, tbl)

        src_conn.close()

    except Exception as e:
        print("Data Extract error:", e)


# ==============================
# LOAD FUNCTION
# ==============================
def load(df, tbl):
    try:
        print(f"Loading table {tbl} into PostgreSQL...")

        engine = create_engine(
            f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        )

        df.to_sql(f"stg_{tbl}", engine, if_exists="replace", index=False)

        print(f"Table stg_{tbl} successfully loaded ({len(df)} rows).")

    except Exception as e:
        print("Data Load error:", e)


# ==============================
# MAIN EXECUTION
# ==============================
if __name__ == "__main__":
    print("Starting ETL job...\n")
    extract()
    print("\nETL job completed.")
