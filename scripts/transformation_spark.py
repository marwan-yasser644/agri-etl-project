import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BRONZE_PATH = "/opt/airflow/data/bronze/sensor_data"
GOLD_PATH = "/opt/airflow/data/gold"
OUTPUT_PATH = "/opt/airflow/data/output"

# =========================
# Validate input data
# =========================
if not os.path.isdir(BRONZE_PATH):
    print(f"Bronze path does not exist: {BRONZE_PATH}. Skipping transformation.")
    sys.exit(0)

parquet_files = [f for f in os.listdir(BRONZE_PATH) if f.endswith('.parquet')]
if not parquet_files:
    print(f"No parquet files found in {BRONZE_PATH}. Skipping transformation.")
    sys.exit(0)

os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

try:
    spark = SparkSession.builder \
        .appName('StarSchemaTransformations') \
        .master('local[*]') \
        .getOrCreate()

    # =========================
    # Read Bronze
    # =========================
    df = spark.read.parquet(BRONZE_PATH)

    # =========================
    # Transformations
    # =========================
    df = df.withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )

    dim_sensor = df.select("sensor_id").distinct()

    fact_table = df.withColumn(
        "date_key",
        F.date_format("event_time", "yyyyMMdd")
    )

    # =========================
    # Save Gold Layer (Parquet)
    # =========================
    dim_sensor.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_sensor"
    )

    fact_table.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/fact_sensor_readings"
    )

    # =========================
    # 🔥 NEW: Export for Snowflake
    # =========================
    fact_pandas = fact_table.select(
        "sensor_id",
        "Temperature",
        "Humidity",
        "Rainfall"
    ).toPandas()

    output_file = f"{OUTPUT_PATH}/transformed_data.csv"
    fact_pandas.to_csv(output_file, index=False)

    print(f"✅ Gold Layer Created Successfully")
    print(f"📦 Snowflake-ready file saved: {output_file}")

except Exception as e:
    print(f"❌ Transformation Error: {e}")

finally:
    if 'spark' in globals():
        spark.stop()