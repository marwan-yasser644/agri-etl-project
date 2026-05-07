import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

BRONZE_PATH = "/opt/airflow/data/bronze/sensor_data"
GOLD_PATH = "/opt/airflow/data/gold"
OUTPUT_PATH = "/opt/airflow/data"

# =========================
# Validate input data
# =========================
if not os.path.isdir(BRONZE_PATH):
    print(f"❌ Bronze path does not exist: {BRONZE_PATH}")
    sys.exit(1)

parquet_files = [f for f in os.listdir(BRONZE_PATH) if f.endswith('.parquet')]

if not parquet_files:
    print(f"❌ No parquet files found in {BRONZE_PATH}")
    sys.exit(1)

os.makedirs(GOLD_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

spark = None

try:
    # =========================
    # Create Spark Session
    # =========================
    spark = SparkSession.builder \
        .appName("StarSchemaTransformations") \
        .master("local[*]") \
        .getOrCreate()

    print("✅ Spark Session Started")

    # =========================
    # Read Bronze Layer
    # =========================
    df = spark.read.parquet(BRONZE_PATH)

    print("✅ Bronze data loaded")

    # =========================
    # Transformations
    # =========================
    df = df.withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )

    # =========================
    # Dimension Table
    # =========================
    dim_sensor = df.select("sensor_id").distinct()

    # =========================
    # Fact Table
    # =========================
    fact_table = df.withColumn(
        "date_key",
        F.date_format("event_time", "yyyyMMdd")
    )

    # =========================
    # Save Gold Layer
    # =========================
    dim_sensor.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_sensor"
    )

    fact_table.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/fact_sensor_readings"
    )

    print("✅ Gold Layer Saved")

    # =========================
    # Export CSV for Snowflake
    # =========================
    fact_pandas = fact_table.select(
        "sensor_id",
        "Temperature",
        "Humidity",
        "Rainfall"
    ).toPandas()

    output_file = f"{OUTPUT_PATH}/transformed_data.csv"

    fact_pandas.to_csv(output_file, index=False)

    print(f"✅ CSV exported successfully")
    print(f"📦 File location: {output_file}")

    # =========================
    # Verify File Exists
    # =========================
    if os.path.exists(output_file):
        print("✅ transformed_data.csv exists")
    else:
        print("❌ transformed_data.csv NOT found after export")
        sys.exit(1)

except Exception as e:
    print(f"❌ Transformation Error: {e}")
    sys.exit(1)

finally:
    if spark:
        spark.stop()
        print("🛑 Spark Session Stopped")