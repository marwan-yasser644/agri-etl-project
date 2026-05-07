import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName('StarSchemaTransformations') \
    .master('local[*]') \
    .getOrCreate()

BRONZE_PATH = "data/bronze/sensor_data/"
GOLD_PATH = "data/gold/"

os.makedirs(GOLD_PATH, exist_ok=True)

try:

    df = spark.read.parquet(BRONZE_PATH)

    df = df.withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )

    dim_sensor = df.select("sensor_id").distinct()

    fact_table = df.withColumn(
        "date_key",
        F.date_format("event_time", "yyyyMMdd")
    )

    dim_sensor.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_sensor"
    )

    fact_table.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/fact_sensor_readings"
    )

    print("Gold Layer Created Successfully")

except Exception as e:
    print(f"Transformation Error: {e}")

finally:
    spark.stop()