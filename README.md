# Agri-IoT Data Pipeline (ETL) 🌿🚜

An automated ETL pipeline designed to ingest, process, and analyze smart agriculture sensor data. This project leverages **Apache Airflow** for orchestration, **PySpark** for data transformation, and **Snowflake** as the final data warehouse.

## 🚀 Project Overview
The goal of this project is to simulate real-time agricultural sensor pings (temperature, humidity, soil moisture) and move them through a professional data engineering lifecycle:
1.  **Simulate:** Generate synthetic IoT sensor data in JSON format.
2.  **Extract:** Move raw data to a centralized storage (HDFS/Local Data Lake).
3.  **Transform:** Clean and aggregate data using PySpark.
4.  **Load:** Bulk load the processed data into Snowflake for analytics.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow
* **Processing:** PySpark
* **Storage:** Snowflake (Cloud Data Warehouse)
* **Infrastructure:** Docker & Docker Compose
* **Language:** Python

## 🏗️ Architecture
The pipeline follows the **Medallion Architecture** logic:
* **Bronze:** Raw sensor pings (`.json` files).
* **Silver:** Transformed CSV data after cleaning.
* **Gold:** Final tables in Snowflake (`AGRI_DATA_DB.GOLD_LAYER`).

## 📊 DAG Workflow
The Airflow DAG `agri_data_pipeline` consists of the following tasks:
- `simulate_data`: Generates IoT pings.
- `extract_to_hdfs`: Moves files to the staging area.
- `transform_data`: Runs Spark jobs for processing.
- `load_to_snowflake`: Final delivery to the cloud.

## 🛠️ How to Run
1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/marwan-yasser644/agri-etl-project.git](https://github.com/marwan-yasser644/agri-etl-project.git)
    ```
2.  **Spin up the environment:**
    ```bash
    docker-compose up -d
    ```
3.  **Access Airflow:**
    Open `localhost:8080` and trigger the `agri_data_pipeline` DAG.

## 📈 Future Improvements
* Implement Data Quality checks using Great Expectations.
* Build a real-time dashboard using **Streamlit**.
* Add Slack notifications for pipeline failures.


