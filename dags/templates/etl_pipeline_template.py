from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    pass

def transform():
    pass

def load():
    pass

default_args = {
    "owner": "data-engineering",
    "retries": 2
}

with DAG(
    dag_id="etl_pipeline_template",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl","template"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task