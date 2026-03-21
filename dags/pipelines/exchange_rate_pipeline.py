from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta
import pendulum
import pandas as pd
import logging

# Import refactored functions
from utils.api_client import fetch_exchange_rates
from utils.db_manager import load_df_to_db
from utils.data_quality import check_null_rates_for_date
from utils.reporting import create_csv_report, generate_html_summary, generate_html_summary_from_df
from utils.alerting import discord_alert

MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "global_exchange_rates"
API_URL = "https://api.exchangerate-api.com/v4/latest/THB"

# Set local timezone for consistent date handling (especially for email content)
local_tz = pendulum.timezone("Asia/Bangkok")


def _extract_and_load(**context):
    """Extract, update schema, and load data."""
    df = fetch_exchange_rates(API_URL)

    if df.empty:
        logging.warning("No data fetched from API. Pushing empty dataframe to XComs.")
        context['ti'].xcom_push(key='report_df_json', value=df.to_json())
        return
    # Add updated_at timestamp
    df['updated_at'] = pendulum.now(local_tz).naive()

    # Add columns to match the old structure for compatibility if needed
    df['period'] = df['rate_date']
    df['currency'] = df['currency_code']

    # Get the actual date from data to use in downstream tasks
    data_date = df['rate_date'].iloc[0] if not df.empty else context['ds']

    load_df_to_db(df, TABLE_NAME, MYSQL_CONN_ID)
    
    # Push data to XComs for downstream tasks to ensure idempotency
    context['ti'].xcom_push(key='report_df_json', value=df.to_json())
    context['ti'].xcom_push(key='execution_date_str', value=str(data_date))


def _data_quality_check(**context):
    """Perform data quality check for the current run date."""
    execution_date = context['ti'].xcom_pull(key='execution_date_str')
    if execution_date:
        check_null_rates_for_date(TABLE_NAME, MYSQL_CONN_ID, execution_date)


def _prepare_email(**context):
    """Create CSV and HTML content for the email from XCom data."""
    df_json = context['ti'].xcom_pull(task_ids='extract_and_load', key='report_df_json')
    data_date = context['ti'].xcom_pull(task_ids='extract_and_load', key='execution_date_str') or context['ds']

    # If no JSON is passed, it means the fetch failed catastrophically before even pushing an empty df
    if df_json is None:
        logging.error("Did not receive any data from upstream. Aborting report generation.")
        raise ValueError("No data received from extract_and_load task.")

    df_report = pd.read_json(df_json)
    output_path = f"/tmp/global_exchange_rate_{data_date}.csv"

    if df_report.empty:
        logging.info("Upstream task passed an empty dataframe. Generating an empty report.")
        with open(output_path, 'w') as f:
            f.write("No data available for this date.")
        html_content = f"<p>No data available for {data_date}.</p>"
    else:
        # Generate CSV from the dataframe passed via XCom
        report_cols = ['rate_date', 'period', 'currency', 'currency_code', 'rate']
        cols_to_select = [col for col in report_cols if col in df_report.columns]
        df_report[cols_to_select].to_csv(output_path, index=False)

        # Generate HTML from the dataframe passed via XCom to ensure consistency
        html_content = generate_html_summary_from_df(df_report, data_date)

    context['ti'].xcom_push(key='html_report', value=html_content)
    context['ti'].xcom_push(key='report_file', value=output_path)


with DAG(
    dag_id="exchange_rate_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz), 
    schedule='@daily', # Run every day at midnight to capture the latest rates
    catchup=False,
    default_args={
        "email": ["global_fx_rate@email.com"], # <--- recipients for email alerts
        "email_on_failure": True,              # <--- enable email alerts on failure
        "retries": 3,
        "on_failure_callback": discord_alert,
        "retry_delay":timedelta(minutes=10)
    },
    tags=["global", "exchange", "finance", "etl"]
) as dag:

    extract_and_load_task = PythonOperator(
        task_id="extract_and_load",
        python_callable=_extract_and_load,
    )

    data_quality_check_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=_data_quality_check,
    )

    prepare_email_task = PythonOperator(
        task_id="prepare_email",
        python_callable=_prepare_email,
    )

    email = EmailOperator(
        task_id="send_email",
        to="global_fx_rate@email.com",
        subject="Global FX Rate Report {{ ti.xcom_pull(task_ids='extract_and_load', key='execution_date_str') }}",
        html_content="{{ task_instance.xcom_pull(task_ids='prepare_email', key='html_report') }}",
        files=["{{ task_instance.xcom_pull(task_ids='prepare_email', key='report_file') }}"],
    )

    extract_and_load_task >> data_quality_check_task >> prepare_email_task >> email