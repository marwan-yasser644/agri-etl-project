import pandas as pd
import time
import os
from datetime import datetime
import random

SOURCE_CSV = 'data/Agri_yield_prediction.csv'
STREAMING_LANDING_ZONE = "data/raw_sensor_pings/"

SENSOR_COLS = [
    'Temperature', 'Humidity', 'Rainfall', 'pH', 'EC',
    'Solar_Radiation', 'Wind_Speed', 'NDVI', 'EVI'
]

os.makedirs(STREAMING_LANDING_ZONE, exist_ok=True)

def run_simulator():
    try:
        df = pd.read_csv(SOURCE_CSV)
        df.columns = df.columns.str.strip()

        missing = [c for c in SENSOR_COLS if c not in df.columns]

        if missing:
            print(f"Missing sensor columns in CSV: {missing}")
            return

        print("Data Loaded Successfully")

    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    batch_size = 5

    for i in range(0, len(df), batch_size):

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

        time.sleep(2)

if __name__ == "__main__":
    run_simulator()