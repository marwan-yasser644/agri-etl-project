import pandas as pd
import time
import os
import sys
from datetime import datetime
import random

SOURCE_CSV = '/opt/airflow/data/Agri_yield_prediction.csv'
STREAMING_LANDING_ZONE = "/opt/airflow/data/raw_sensor_pings"

SENSOR_COLS = [
    'Temperature', 'Humidity', 'Rainfall', 'pH', 'EC',
    'Solar_Radiation', 'Wind_Speed', 'NDVI', 'EVI'
]

os.makedirs(STREAMING_LANDING_ZONE, exist_ok=True)

def run_simulator():
    try:
        if not os.path.exists(SOURCE_CSV):
            print(f"Source CSV not found: {SOURCE_CSV}")
            return False
        
        df = pd.read_csv(SOURCE_CSV)
        df.columns = df.columns.str.strip()

        missing = [c for c in SENSOR_COLS if c not in df.columns]

        if missing:
            print(f"Missing sensor columns in CSV: {missing}")
            return False

        print("Data Loaded Successfully")

    except Exception as e:
        print(f"Error loading CSV: {e}")
        return False

    batch_size = 50  # Each batch has 50 rows

    try:
        batch_count = 0
        max_batches = 2  # Limit to 2 batches for testing
        
        for i in range(0, len(df), batch_size):
            if batch_count >= max_batches:
                break
                
            chunk = df.iloc[i:i+batch_size][SENSOR_COLS].copy()

            sensor_id = random.randint(1, 10)

            chunk['sensor_id'] = f'AGRI-IOT-{sensor_id}'
            chunk['event_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            file_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')

            file_path = os.path.join(
                STREAMING_LANDING_ZONE,
                f"sensor_ping_{file_id}.json"
            )

            chunk.to_json(file_path, orient='records', lines=True)

            print(f"Batch Sent => {file_path}")
            batch_count += 1

            time.sleep(1)
        
        return True
    except Exception as e:
        print(f"Error writing batches: {e}")
        return False

if __name__ == "__main__":
    success = run_simulator()
    sys.exit(0 if success else 1)
