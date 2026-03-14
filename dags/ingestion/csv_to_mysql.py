from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine
import logging
import pendulum

# Set local timezone for consistent date handling
local_tz = pendulum.timezone("Asia/Bangkok")



CSV_FILE_PATH = "/opt/airflow/data/people.csv"
# CSV_FILE_PATH = "/opt/airflow/data/raw_customer_reviews.csv"
TABLE_NAME = "people"
# TABLE_NAME = "raw_customer_reviews"

def load_csv_to_mysql():
    df = pd.read_csv(CSV_FILE_PATH)

    connection = BaseHook.get_connection("mysql_default")

    engine = create_engine(
        f"mysql+pymysql://{connection.login}:{connection.password}"
        f"@{connection.host}:{connection.port}/{connection.schema}"
    )

    with engine.begin() as conn:
        df.to_sql(name=TABLE_NAME, con=conn, if_exists="replace", index=False)

    logging.info(f"Loaded {len(df)} rows into {TABLE_NAME}")


default_args = {
    "retries": 1,
}

with DAG(
    dag_id="csv_to_mysql",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["demo"],
) as dag:

    load_csv_task = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_csv_to_mysql,
    )