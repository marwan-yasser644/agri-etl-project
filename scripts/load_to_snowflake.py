import snowflake.connector
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SnowflakeLoad") \
    .master("local[*]") \
    .getOrCreate()

sf_options = {
    "sfURL": "LLYHYMM-YJ95431.snowflakecomputing.com",
    "sfUser": "MARWANYASSER",
    "sfPassword": "Marwan_Mero_22",
    "sfDatabase": "AGRI_DATA_DB",
    "sfSchema": "GOLD_LAYER",
    "sfWarehouse": "AGRI_WH"
}

try:

    conn = snowflake.connector.connect(
        user=sf_options["sfUser"],
        password=sf_options["sfPassword"],
        account="LLYHYMM-YJ95431",
        warehouse=sf_options["sfWarehouse"],
        database=sf_options["sfDatabase"],
        schema=sf_options["sfSchema"]
    )

    print("Connected To Snowflake Successfully")

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS SENSOR_DATA (
        sensor_id STRING,
        Temperature FLOAT,
        Humidity FLOAT,
        Rainfall FLOAT
    )
    """)

    print("Table Created Successfully")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Snowflake Error: {e}")