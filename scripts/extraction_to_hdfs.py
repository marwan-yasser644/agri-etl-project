import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

os.environ["HADOOP_USER_NAME"] = "root"

spark = SparkSession.builder \
    .appName('AirFlowBatchProcessingJob') \
    .master('local[*]') \
    .getOrCreate()

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

input_path = "data/raw_sensor_pings/"
output_path = "data/bronze/sensor_data/"

os.makedirs(output_path, exist_ok=True)

try:

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
    spark.stop()