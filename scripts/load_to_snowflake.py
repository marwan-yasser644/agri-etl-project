import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ==========================================
# Snowflake Credentials (بياناتك المحدثة)
# ==========================================
sf_user = 'MARWANYASSER'
sf_password = 'Marwan_Mero_22'
sf_account = 'AUNSOPN-RH43826'  # المعرف الصحيح من صورك
sf_database = 'AGRI_DATA_DB'
sf_schema = 'GOLD_LAYER'
sf_warehouse = 'AGRI_WH'

try:
    print(f"🔗 Connecting to Snowflake: {sf_account}...")
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse=sf_warehouse,
        database=sf_database,
        schema=sf_schema
    )
    print("✅ Connected successfully!")

    # 1. فحص مسار ملف البيانات
    # سنبحث في المسار المطلق والمسار المؤقت لضمان الوصول للملف
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
        print("❌ Error: transformed_data.csv NOT found in any expected location.")
        sys.exit(1)

    print(f"📂 Found data at: {data_path}")

    # 2. معالجة البيانات
    df = pd.read_csv(data_path)
    df.columns = [col.upper() for col in df.columns] # توحيد الأعمدة لـ Uppercase لتطابق Snowflake
    
    # 3. الرفع لـ Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="SENSOR_DATA",
        database=sf_database,
        schema=sf_schema
    )

    if success:
        print(f"🚀 SUCCESS: {nrows} rows loaded into SENSOR_DATA!")
    else:
        print("⚠️ Loading failed or partial success.")

    conn.close()

except Exception as e:
    print(f"❌ Critical Error: {str(e)}")
    sys.exit(1)