from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'marwan',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='agri_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    simulate_data = BashOperator(
        task_id='simulate_data',
        bash_command='python /opt/airflow/scripts/simulate_data.py'
    )

    extraction = BashOperator(
        task_id='extract_to_hdfs',
        bash_command='python /opt/airflow/scripts/extraction_to_hdfs.py'
    )

    transform = BashOperator(
        task_id='transform_data',
        bash_command='python /opt/airflow/scripts/transformation_spark.py'
    )

    load = BashOperator(
        task_id='load_to_snowflake',
        bash_command='python /opt/airflow/scripts/load_to_snowflake.py'
    )

    simulate_data >> extraction >> transform >> load