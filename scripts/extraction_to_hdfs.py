import os
import sys
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

input_path = "/opt/airflow/data/raw_sensor_pings"
output_path = "/opt/airflow/data/bronze/sensor_data"

os.environ["HADOOP_USER_NAME"] = "root"

if not os.path.isdir(input_path):
    print(f"Input path does not exist: {input_path}. Skipping extraction.")
    sys.exit(0)

json_files = glob.glob(os.path.join(input_path, "*.json"))
if not json_files:
    print(f"No JSON files found in {input_path}. Skipping extraction.")
    sys.exit(0)

os.makedirs(output_path, exist_ok=True)

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Rainfall", DoubleType(), True),
    StructField("pH", DoubleType(), True),
    StructField("EC", DoubleType(), True),
    StructField("Solar_Radiation", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("NDVI", DoubleType(), True),
    StructField("EVI", DoubleType(), True)
])

try:
    spark = SparkSession.builder \
        .appName('AirFlowBatchProcessingJob') \
        .master('local[*]') \
        .getOrCreate()

    raw_df = spark.read \
        .schema(schema) \
        .json(input_path)

    record_count = raw_df.count()

    if record_count > 0:
        raw_df.write \
            .mode("append") \
            .parquet(output_path)
        print(f"Processed {record_count} records")
    else:
        print("No data found")

except Exception as e:
    print(f"Error: {e}")

finally:
    if 'spark' in globals():
        spark.stop()