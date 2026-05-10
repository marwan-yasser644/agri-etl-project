# 🌾 Agri-IoT Data Pipeline

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)

A production-grade ETL pipeline engineered to ingest, process, and analyze IoT sensor data from smart agriculture systems. This project demonstrates modern data engineering practices using industry-standard tools to build a scalable, automated data pipeline from raw sensor readings to actionable insights.

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Architecture](#-architecture)
- [Data Flow](#-data-flow)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Running the Pipeline](#-running-the-pipeline)
- [Airflow Usage](#-airflow-usage)
- [Snowflake Integration](#-snowflake-integration)
- [Monitoring & Logs](#-monitoring--logs)
- [Future Enhancements](#-future-enhancements)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Project Overview

The **Agri-IoT Data Pipeline** simulates a real-world scenario where agricultural IoT sensors continuously monitor environmental conditions (temperature, humidity, soil moisture) across farmlands. The pipeline automates the entire data lifecycle—from raw data ingestion to analytics-ready tables in a cloud data warehouse.

### Business Use Case

Modern precision agriculture relies on continuous monitoring of crop conditions. This pipeline enables:
- **Real-time data aggregation** from distributed sensor networks
- **Historical trend analysis** for crop yield optimization
- **Anomaly detection** for early warning systems
- **Data-driven decision making** for irrigation and climate control

---

## ✨ Features

- **🔄 End-to-End Automation**: Fully orchestrated ETL workflow using Apache Airflow
- **⚡ Distributed Processing**: PySpark for scalable data transformation
- **☁️ Cloud-Native Storage**: Snowflake as the analytical data warehouse
- **🏗️ Medallion Architecture**: Bronze → Silver → Gold layered data structure
- **🐳 Containerized Deployment**: Docker Compose for reproducible environments
- **📊 Sensor Simulation**: Realistic IoT data generation for testing
- **🔍 Data Quality**: Schema validation and data cleaning pipelines
- **📈 Scalable Design**: Handles batch processing with room for streaming enhancements

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.x | Workflow scheduling and monitoring |
| **Data Processing** | PySpark 3.x | Distributed data transformation |
| **Data Warehouse** | Snowflake | Cloud-based analytics storage |
| **Containerization** | Docker & Docker Compose | Environment isolation and deployment |
| **Language** | Python 3.9+ | Core programming language |
| **Storage Layer** | HDFS / Local FS | Intermediate data staging |

---

## 🏗️ Architecture

The pipeline implements the **Medallion Architecture** pattern for data quality and progressive refinement:

```
┌─────────────────┐
│  IoT Sensors    │  (Simulated JSON data)
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────┐
│              Apache Airflow DAG                      │
│  ┌──────────────────────────────────────────────┐   │
│  │  1. simulate_data                            │   │
│  │     └─> Generate sensor pings (JSON)         │   │
│  └──────────────────┬───────────────────────────┘   │
│                     ▼                                │
│  ┌──────────────────────────────────────────────┐   │
│  │  2. extract_to_hdfs                          │   │
│  │     └─> Move to Bronze Layer (Raw Data)      │   │
│  └──────────────────┬───────────────────────────┘   │
│                     ▼                                │
│  ┌──────────────────────────────────────────────┐   │
│  │  3. transform_data (PySpark)                 │   │
│  │     └─> Clean & Aggregate (Silver Layer)     │   │
│  └──────────────────┬───────────────────────────┘   │
│                     ▼                                │
│  ┌──────────────────────────────────────────────┐   │
│  │  4. load_to_snowflake                        │   │
│  │     └─> Load to Gold Layer (Analytics)       │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Snowflake Database    │
         │  AGRI_DATA_DB.GOLD     │
         └────────────────────────┘
```

### Data Layers

- **🥉 Bronze Layer**: Raw, unprocessed sensor data in JSON format
- **🥈 Silver Layer**: Cleaned and validated CSV data with schema enforcement
- **🥇 Gold Layer**: Aggregated, business-ready tables in Snowflake

---

## 🔄 Data Flow

### 1. Data Generation (Simulate)
```python
# Sample sensor ping structure
{
  "sensor_id": "S001",
  "timestamp": "2024-05-10T14:30:00Z",
  "temperature": 24.5,
  "humidity": 65.2,
  "soil_moisture": 45.8,
  "location": "Field_A"
}
```

### 2. Extraction (Bronze)
- Raw JSON files stored in `/data/bronze/`
- Timestamped batches for incremental processing
- Preservation of original data for audit trails

### 3. Transformation (Silver)
PySpark jobs perform:
- Schema validation and type casting
- Null value handling and outlier detection
- Timestamp standardization
- Data deduplication
- Aggregation by sensor and time windows

### 4. Loading (Gold)
- Bulk insert to Snowflake using Snowflake Connector
- Partitioned tables for query optimization
- Incremental updates to prevent duplicates

---

## 📁 Project Structure

```
agri-etl-project/
│
├── dags/
│   └── agri_data_pipeline.py       # Main Airflow DAG definition
│
├── scripts/
│   ├── simulate_data.py             # IoT sensor data generator
│   ├── extract_to_hdfs.py           # File movement logic
│   ├── transform_data.py            # PySpark transformation jobs
│   └── load_to_snowflake.py         # Snowflake loader
│
├── data/
│   ├── bronze/                      # Raw JSON sensor data
│   ├── silver/                      # Cleaned CSV files
│   └── gold/                        # (Optional local backup)
│
├── config/
│   ├── airflow.cfg                  # Airflow configuration
│   └── snowflake_credentials.json   # Snowflake connection details
│
├── docker-compose.yml               # Multi-container orchestration
├── Dockerfile                       # Custom Airflow image
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment variables template
└── README.md                        # Project documentation
```

---

## 📦 Prerequisites

Before running the pipeline, ensure you have:

- **Docker** (v20.10+) and **Docker Compose** (v2.0+)
- **Snowflake Account** with appropriate privileges
- **Git** for cloning the repository
- Minimum **4GB RAM** allocated to Docker

---

## 🚀 Installation

### Step 1: Clone the Repository

```bash
git clone https://github.com/marwan-yasser644/agri-etl-project.git
cd agri-etl-project
```

### Step 2: Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` and add your Snowflake credentials:

```env
# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=AGRI_DATA_DB
SNOWFLAKE_SCHEMA=GOLD_LAYER

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Step 3: Build and Start Containers

```bash
docker-compose up -d --build
```

This will start:
- Apache Airflow (webserver + scheduler)
- PostgreSQL (Airflow metadata database)
- PySpark executor environment

### Step 4: Initialize Airflow Database

```bash
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

## ▶️ Running the Pipeline

### Access Airflow Web UI

Navigate to [http://localhost:8080](http://localhost:8080)

- **Username**: `admin`
- **Password**: `admin` (or as configured)

### Trigger the DAG

1. Locate the `agri_data_pipeline` DAG in the dashboard
2. Toggle the DAG to **ON**
3. Click **Trigger DAG** (play button)
4. Monitor task execution in the Graph or Tree view

### Manual Execution (CLI)

```bash
# Trigger the DAG manually
docker-compose exec airflow-webserver airflow dags trigger agri_data_pipeline

# Check DAG status
docker-compose exec airflow-webserver airflow dags list-runs -d agri_data_pipeline
```

---

## 🌬️ Airflow Usage

### DAG Configuration

The pipeline runs on a configurable schedule (default: daily at 2 AM UTC):

```python
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'agri_data_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)
```

### Task Dependencies

```python
simulate_data >> extract_to_hdfs >> transform_data >> load_to_snowflake
```

### Viewing Logs

Access task logs through the Airflow UI:
1. Click on the task instance
2. Select **Log** tab
3. View stdout/stderr for debugging

---

## ❄️ Snowflake Integration

### Database Setup

Run this SQL in your Snowflake worksheet to prepare the environment:

```sql
-- Create database and schema
CREATE DATABASE IF NOT EXISTS AGRI_DATA_DB;
CREATE SCHEMA IF NOT EXISTS AGRI_DATA_DB.GOLD_LAYER;

-- Create sensor data table
CREATE OR REPLACE TABLE AGRI_DATA_DB.GOLD_LAYER.SENSOR_READINGS (
    sensor_id VARCHAR(50),
    reading_timestamp TIMESTAMP_NTZ,
    temperature FLOAT,
    humidity FLOAT,
    soil_moisture FLOAT,
    location VARCHAR(100),
    ingestion_date DATE,
    PRIMARY KEY (sensor_id, reading_timestamp)
);

-- Create aggregated metrics table
CREATE OR REPLACE TABLE AGRI_DATA_DB.GOLD_LAYER.DAILY_METRICS (
    location VARCHAR(100),
    metric_date DATE,
    avg_temperature FLOAT,
    avg_humidity FLOAT,
    avg_soil_moisture FLOAT,
    min_temperature FLOAT,
    max_temperature FLOAT,
    reading_count INT,
    PRIMARY KEY (location, metric_date)
);
```

### Connection Configuration

The pipeline uses the Snowflake Python Connector:

```python
import snowflake.connector

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
```

### Querying Results

```sql
-- View latest sensor readings
SELECT * FROM AGRI_DATA_DB.GOLD_LAYER.SENSOR_READINGS
ORDER BY reading_timestamp DESC
LIMIT 100;

-- Analyze daily trends
SELECT 
    location,
    metric_date,
    avg_temperature,
    avg_soil_moisture
FROM AGRI_DATA_DB.GOLD_LAYER.DAILY_METRICS
WHERE metric_date >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY location, metric_date;
```

---

## 📊 Monitoring & Logs

### Container Logs

```bash
# View all container logs
docker-compose logs -f

# View Airflow scheduler logs
docker-compose logs -f airflow-scheduler

# View Airflow webserver logs
docker-compose logs -f airflow-webserver
```

### Data Quality Checks

Monitor these key metrics:
- **Row counts** at each layer (Bronze → Silver → Gold)
- **Null percentages** in critical columns
- **Duplicate records** after deduplication
- **Processing time** per batch

---

## 🚀 Future Enhancements

### Short-term Goals
- [ ] **Data Quality Framework**: Integrate Great Expectations for automated validation
- [ ] **Alerting System**: Add Slack/Email notifications for pipeline failures
- [ ] **Interactive Dashboard**: Build Streamlit dashboard for real-time monitoring
- [ ] **Unit Tests**: Add pytest coverage for transformation logic

### Long-term Vision
- [ ] **Streaming Pipeline**: Migrate to Apache Kafka for real-time processing
- [ ] **ML Integration**: Add predictive models for crop yield forecasting
- [ ] **Multi-Cloud Support**: Extend to AWS S3 + Redshift / Azure Synapse
- [ ] **Data Catalog**: Implement Apache Atlas for metadata management
- [ ] **CI/CD Pipeline**: GitHub Actions for automated testing and deployment

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Marwan Yasser**

- GitHub: [@marwan-yasser644](https://github.com/marwan-yasser644)
- LinkedIn: [Add your LinkedIn profile]

---

## 🙏 Acknowledgments

- Apache Airflow community for excellent orchestration tools
- Snowflake for providing robust cloud data warehousing
- The open-source data engineering community

---

## 📸 Screenshots

### Airflow DAG Graph View
*[Add screenshot of your DAG execution graph]*

### Snowflake Data Preview
*[Add screenshot of query results from Snowflake]*

### Architecture Diagram
*[Add detailed architecture diagram if available]*

---

**⭐ If you found this project helpful, please consider giving it a star!**
