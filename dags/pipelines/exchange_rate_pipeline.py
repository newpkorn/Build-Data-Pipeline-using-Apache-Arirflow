from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta

# Import refactored functions
from utils.api_client import fetch_exchange_rates
from utils.db_manager import update_schema, load_df_to_db
from utils.data_quality import check_null_rates_for_date
from utils.reporting import create_csv_report, generate_html_summary
from utils.alerting import discord_alert

MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "global_exchange_rates"
API_URL = "https://api.exchangerate-api.com/v4/latest/THB"


def _extract_and_load(**context):
    """Extract, update schema, and load data."""
    df = fetch_exchange_rates(API_URL)
    
    # Add columns to match the old structure for compatibility if needed
    df['period'] = df['rate_date']
    df['currency'] = df['currency_code']
    
    update_schema(df, TABLE_NAME, MYSQL_CONN_ID)
    load_df_to_db(df, TABLE_NAME, MYSQL_CONN_ID)
    
    # Push the execution date for downstream tasks
    context['ti'].xcom_push(key='execution_date_str', value=context['ds'])


def _data_quality_check(**context):
    """Perform data quality check for the current run date."""
    execution_date = context['ti'].xcom_pull(key='execution_date_str')
    if execution_date:
        check_null_rates_for_date(TABLE_NAME, MYSQL_CONN_ID, execution_date)


def _prepare_email(**context):
    """Create CSV and HTML content for the email."""
    ds = context['ds']
    output_path = f"/tmp/global_exchange_rate_{ds}.csv"
    
    create_csv_report(output_path, TABLE_NAME, MYSQL_CONN_ID)
    html_content = generate_html_summary(TABLE_NAME, MYSQL_CONN_ID, ds)
    
    context['ti'].xcom_push(key='html_report', value=html_content)


with DAG(
    dag_id="bot_fx_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="0 13 * * 1-5",
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
        subject="Global FX Rate Report {{ ds }}",
        html_content="{{ task_instance.xcom_pull(task_ids='prepare_email', key='html_report') }}",
        files=[f"/tmp/global_exchange_rate_{{{{ ds }}}}.csv"],
    )

    extract_and_load_task >> data_quality_check_task >> prepare_email_task >> email