from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable

# from utils.bot_api_client import fetch_bot_rates
# from utils.schema_manager import sync_schema
# from utils.data_quality import check_null_rates
# from utils.alerting import slack_alert

from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import json
import csv

MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "bot_exchange_rates"

BOT_API = "https://api.exchangerate-api.com/v4/latest/THB"

OUTPUT_FILE = "/tmp/bot_rates.csv"


def fetch_bot_rate(**context):

    r = requests.get(BOT_API)
    if r.status_code != 200:
        raise ValueError(f"API failed with status {r.status_code}: {r.text}")

    data = r.json()

    if "rates" not in data:
        raise KeyError(f"'rates' key not found in API response: {data}")

    rates = data["rates"]

    rows = []
    for currency, rate in rates.items():

        rows.append({
            "rate_date": data["date"],
            "period": data["date"],
            "currency": currency,
            "currency_code": currency,
            "rate": rate
        })

    df = pd.DataFrame(rows)

    context["ti"].xcom_push(key="df", value=df.to_json())


def detect_and_update_schema(**context):

    df = pd.read_json(context["ti"].xcom_pull(key="df"))

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT AUTO_INCREMENT PRIMARY KEY
    )
    """)

    cursor.execute(f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{TABLE_NAME}'
    """)

    existing_cols = [c[0] for c in cursor.fetchall()]

    for col in df.columns:

        if col not in existing_cols:

            cursor.execute(
                f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} TEXT"
            )

    conn.commit()


def insert_data(**context):

    df = pd.read_json(context["ti"].xcom_pull(key="df"))

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Clear existing data for the date to prevent duplicates
    if not df.empty and "rate_date" in df.columns:
        dates = df["rate_date"].unique()
        for d in dates:
            cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE rate_date = %s", (d,))

    for _, row in df.iterrows():

        cursor.execute(f"""
        INSERT INTO {TABLE_NAME}
        (rate_date,period,currency,currency_code,rate)
        VALUES (%s,%s,%s,%s,%s)
        """, (
            row["rate_date"],
            row["period"],
            row["currency"],
            row["currency_code"],
            row["rate"]
        ))

    conn.commit()


def data_quality_check(**context):

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    records = mysql_hook.get_records(f"""
    SELECT COUNT(*)
    FROM {TABLE_NAME}
    WHERE rate IS NULL
    """)

    if records[0][0] > 0:

        raise ValueError("Data Quality Check Failed")


def export_csv(**context):

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

    df = mysql_hook.get_pandas_df(
        f"SELECT * FROM {TABLE_NAME} ORDER BY rate_date DESC"
    )

    df.to_csv(OUTPUT_FILE, index=False)


with DAG(
    dag_id="bot_fx_pipeline_portfolio",
    start_date=datetime(2024,1,1),
    schedule_interval="0 13 * * 1-5",
    catchup=False,
    default_args={
        "retries":3,
        # "on_failure_callback": slack_alert,
        "retry_delay":timedelta(minutes=10)
    }
) as dag:

    fetch = PythonOperator(
        task_id="fetch_bot_rate",
        python_callable=fetch_bot_rate
    )

    schema = PythonOperator(
        task_id="detect_schema_change",
        python_callable=detect_and_update_schema
    )

    insert = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    dq = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check
    )

    export = PythonOperator(
        task_id="export_csv",
        python_callable=export_csv
    )

    email = EmailOperator(
        task_id="send_email",
        to="test@example.com",
        subject="BOT FX Rate",
        html_content="BOT FX CSV Attached",
        files=[OUTPUT_FILE]
    )

    fetch >> schema >> insert >> dq >> export >> email