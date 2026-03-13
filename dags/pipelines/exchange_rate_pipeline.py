from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta
import pendulum
import logging

# Import refactored functions
from utils.api_client import fetch_exchange_rates
from utils.db_manager import update_schema, load_df_to_db
from utils.data_quality import check_null_rates_for_date
from utils.reporting import create_csv_report, generate_html_summary
from utils.alerting import discord_alert

MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "global_exchange_rates"
API_URL = "https://api.exchangerate-api.com/v4/latest/THB"

# Set local timezone for consistent date handling (especially for email content)
local_tz = pendulum.timezone("Asia/Bangkok")


def _extract_and_load(**context):
    """Extract, update schema, and load data."""
    df = fetch_exchange_rates(API_URL)
    
    # Add updated_at timestamp
    df['updated_at'] = pendulum.now(local_tz).naive()

    # Add columns to match the old structure for compatibility if needed
    df['period'] = df['rate_date']
    df['currency'] = df['currency_code']
    
    # Get the actual date from data to filter report later
    data_date = df['rate_date'].iloc[0] if not df.empty else context['ds']

    # Handle Schema Update error to prevent failure if Primary Key already exists
    try:
        update_schema(df, TABLE_NAME, MYSQL_CONN_ID)
    except Exception as e:
        if '1068' in str(e) or 'Multiple primary key' in str(e):
            logging.warning("Schema update bypassed: Primary key already defined.")
        else:
            raise

    load_df_to_db(df, TABLE_NAME, MYSQL_CONN_ID)
    
    # Push the execution date for downstream tasks
    context['ti'].xcom_push(key='execution_date_str', value=str(data_date))


def _data_quality_check(**context):
    """Perform data quality check for the current run date."""
    execution_date = context['ti'].xcom_pull(key='execution_date_str')
    if execution_date:
        check_null_rates_for_date(TABLE_NAME, MYSQL_CONN_ID, execution_date)


def _prepare_email(**context):
    """Create CSV and HTML content for the email."""
    # Use actual data date from extract task instead of ds
    data_date = context['ti'].xcom_pull(task_ids='extract_and_load', key='execution_date_str') or context['ds']
    output_path = f"/tmp/global_exchange_rate_{data_date}.csv"
    
    # Query only specific date data for the report
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df_report = mysql_hook.get_pandas_df(f"""
                                            SELECT rate_date, period, currency, currency_code, rate
                                            FROM {TABLE_NAME}
                                            WHERE rate_date = '{data_date}'
                                            """)
    df_report.to_csv(output_path, index=False)

    html_content = generate_html_summary(TABLE_NAME, MYSQL_CONN_ID, data_date)
    
    context['ti'].xcom_push(key='html_report', value=html_content)
    context['ti'].xcom_push(key='report_file', value=output_path)


with DAG(
    dag_id="exchange_rate_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz), 
    schedule_interval='0 13 * * 1-5', # Run daily at midnight to capture the latest rates for the day
    catchup=False,
    default_args={
        "email": ["global_fx_rate@email.com"], # <--- recipients for email alerts
        "email_on_failure": True,              # <--- enable email alerts on failure
        "retries": 3,
        "on_failure_callback": discord_alert,
        "retry_delay":timedelta(minutes=10)
    }
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