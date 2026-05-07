import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ==========================================
# Snowflake Credentials
# ==========================================
sf_user = os.getenv("SNOWFLAKE_USER", "MARWANYASSER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD", "Marwan_Mero_22")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT", "AUNSOPN-RH43826")

sf_database = "AGRI_DATA_DB"
sf_schema = "GOLD_LAYER"
sf_warehouse = "AGRI_WH"
sf_table = "SENSOR_DATA"

try:
    # ==========================================
    # Connect to Snowflake
    # ==========================================
    print(f"🔗 Connecting to Snowflake account: {sf_account} ...")

    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )

    print("✅ Connected to Snowflake successfully!")

    # ==========================================
    # Find CSV File
    # ==========================================
    possible_paths = [
        "/tmp/transformed_data.csv",
        "/opt/airflow/data/transformed_data.csv",
        "/workspaces/agri-etl-project/data/transformed_data.csv"
    ]

    data_path = None

    for path in possible_paths:
        if os.path.exists(path):
            data_path = path
            break

    if not data_path:
        print("❌ ERROR: transformed_data.csv NOT found.")
        sys.exit(1)

    print(f"📂 CSV File Found: {data_path}")

    # ==========================================
    # Read CSV
    # ==========================================
    df = pd.read_csv(data_path)

    # Remove spaces from column names
    df.columns = [col.strip().upper() for col in df.columns]

    print("📋 Detected Columns:")
    print(df.columns.tolist())

    print(f"📊 Total Rows: {len(df)}")

    # ==========================================
    # Create Database + Schema
    # ==========================================
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {sf_database}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_schema}")

    cursor.execute(f"USE DATABASE {sf_database}")
    cursor.execute(f"USE SCHEMA {sf_schema}")

    print("✅ Database and Schema ready!")

    # ==========================================
    # Upload Data to Snowflake
    # ==========================================
    print(f"🚀 Uploading data to table: {sf_table}")

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=sf_table,
        database=sf_database,
        schema=sf_schema,
        auto_create_table=True,
        overwrite=False
    )

    # ==========================================
    # Result
    # ==========================================
    if success:
        print("✅ DATA LOAD SUCCESSFUL!")
        print(f"📦 Chunks Uploaded: {nchunks}")
        print(f"📈 Rows Uploaded: {nrows}")
    else:
        print("⚠️ Data upload failed.")

    # ==========================================
    # Close Connections
    # ==========================================
    cursor.close()
    conn.close()

    print("🔒 Snowflake connection closed.")

except Exception as e:
    print("❌ CRITICAL ERROR:")
    print(str(e))
    sys.exit(1)