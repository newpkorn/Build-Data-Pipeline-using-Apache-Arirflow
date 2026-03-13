# Pipeline to fetch exchange rate data from the Bank of Thailand (BOT)
# ETL Process:
# 1. Extract: Fetch Daily Average Exchange Rate data from BOT API
# 2. Transform: Select desired currencies (USD, EUR, JPY, etc.) and convert data types
# 3. Load: Save to MySQL table bot_exchange_rates

# from utils.bot_api_client import fetch_bot_rates
# from utils.schema_manager import sync_schema
# from utils.data_quality import check_null_rates
# from utils.alerting import slack_alert
from utils.alerting import discord_alert

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable
import requests
import logging
import os
import csv

# --- Configuration ---
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "bot_exchange_rates"

# BOT API Configuration
# Register for an API Key at: https://portal.api.bot.or.th/ (New Portal)
# Note: Use the API Key from the new Developer Portal
API_URL = "https://gateway.api.bot.or.th/Stat-ExchangeRate/v2/DAILY_AVG_EXG_RATE/"

# Base Path for CSV files (date will be appended, e.g., _2026-03-06.csv)
CSV_REPORT_BASE_PATH = "/tmp/bot_exchange_rates"

def extract_data(**context):
    """
    Extract: Fetch data from BOT API based on the execution_date
    """
    # Fetch Key inside the function to reduce Airflow Scheduler load and prevent errors if Variable is not created yet
    api_key = Variable.get("bot_api_key", default_var=os.getenv("BOT_API_KEY"))

    if not api_key:
        raise ValueError("❌ API Key for BOT is not set. Please set it in Airflow Variables or Environment Variables.")

    # Determine Data Date based on Run Type
    run_type = context['dag_run'].run_type

    if run_type == 'manual':
        # Manual Run: ds is Today (e.g. 13th). We want Yesterday (12th).
        execution_date = (datetime.strptime(context['ds'], '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # Scheduled Run: ds is already the Start of Interval (e.g. 11th for run on 12th). Use as is.
        execution_date = context['ds']
    
    logging.info(f"Fetching BOT data for date: {execution_date}")
    context["ti"].xcom_push(key="data_date", value=execution_date)
    
    headers = {
        "Authorization": api_key,
        "Accept": "application/json"
    }
    
    params = {
        "start_period": execution_date,
        "end_period": execution_date
    }

    response = requests.get(API_URL, headers=headers, params=params, timeout=30)
    
    # Check for errors
    if response.status_code != 200:
        logging.error(f"API Error: {response.text}")
        # If it's a 404 or no data, it might be acceptable to pass or raise an error based on business logic.
        response.raise_for_status()
        
    data = response.json()
    
    # Check BOT Response structure
    if "result" in data and "data" in data["result"] and "data_detail" in data["result"]["data"]:
        raw_data = data["result"]["data"]["data_detail"]
        logging.info(f"Extracted {len(raw_data)} records.")
        context["ti"].xcom_push(key="raw_bot_data", value=raw_data)
    else:
        logging.warning("No data found in BOT response")
        context["ti"].xcom_push(key="raw_bot_data", value=[])

def transform_data(**context):
    """
    Transform: Filter currencies and convert data
    """
    raw_data = context["ti"].xcom_pull(key="raw_bot_data")
    if not raw_data:
        logging.info("No data to transform")
        return []

    # Desired currencies (remove this filter if you want all)
    # TARGET_CURRENCIES = ["USD", "EUR", "JPY", "GBP", "SGD", "CNY"]
    
    transformed_data = []
    for item in raw_data:
        currency_code = item.get("currency_id")
        period = item.get("period")

        # Data Cleaning: Skip data without a date (Fix Error 1292) or without a Currency Code
        if not period or not currency_code:
            continue
        
        # Filter only desired currencies
        # if currency_code in TARGET_CURRENCIES:  # <-- Comment only the condition
        transformed_data.append({               # <-- Uncomment this part and adjust Indent
            "period": period,
            "currency_code": currency_code,
            "currency_name": item.get("currency_name_th"),
            "buying_rate": float(item.get("buying_transfer", 0) or 0), # Use buying_transfer as primary
            "selling_rate": float(item.get("selling", 0) or 0),
            "mid_rate": float(item.get("mid_rate", 0) or 0),
            "updated_at": datetime.now().isoformat()
        })
            
    logging.info(f"Transformed data: {len(transformed_data)} records")

    # Create filename based on Data Date (from Extract task) not Run Date (ds)
    data_date = context["ti"].xcom_pull(task_ids="extract_bot_data", key="data_date") or context['ds']
    csv_file_path = f"{CSV_REPORT_BASE_PATH}_{data_date}.csv"
    
    # Create CSV file for email attachment
    if transformed_data:
        # If data exists: Use Keys from actual data (Dynamic), no Hardcoding needed
        fieldnames = transformed_data[0].keys()
        with open(csv_file_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transformed_data)
    else:
        # If no data: Create a simple text file indicating no data (prevents FileNotFoundError)
        with open(csv_file_path, "w", encoding="utf-8-sig") as f:
            f.write("No data available for this date.")
            
    context["ti"].xcom_push(key="cleaned_bot_data", value=transformed_data)

def load_data(**context):
    """
    Load: Save to MySQL with Dynamic Schema
    (Create Columns based on Keys from actual data, no Hardcoding)
    """
    data = context["ti"].xcom_pull(key="cleaned_bot_data")
    if not data:
        raise ValueError("❌ No data to load! Pipeline stopped. Please check Date or API response.")
    
    logging.info(f"Preparing to load {len(data)} rows. Sample Date: {data[0].get('period', 'Unknown')}")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Debug: Print Database Name to verify where data is being loaded
    cursor.execute("SELECT DATABASE();")
    current_db = cursor.fetchone()[0]
    logging.info(f"🔌 Connection established. Target Database: '{current_db}'")

    # Self-Healing: Check if the table has a Primary Key (prevents duplicate data from old table versions)
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
    if cursor.fetchone():
        cursor.execute(f"SHOW KEYS FROM {TABLE_NAME} WHERE Key_name = 'PRIMARY'")
        if not cursor.fetchone():
            # Warning only: Do not drop table automatically to prevent data loss
            logging.warning(f"⚠️ Table {TABLE_NAME} exists but missing Primary Key. Upsert might create duplicates.")
            # cursor.execute(f"DROP TABLE {TABLE_NAME}") # <--- Disabled to prevent data loss

    # 1. Analyze data to create Schema
    first_row = data[0]
    columns = list(first_row.keys())

    # Helper: Map Python Type -> MySQL Type
    def get_sql_type(col_name, value):
        if col_name == 'period': return 'DATE'
        if 'rate' in col_name: return 'DECIMAL(10,4)' # Catch buying_rate, selling_rate
        if 'updated_at' in col_name: return 'TIMESTAMP'
        if isinstance(value, int): return 'INT'
        if isinstance(value, float): return 'DECIMAL(10,4)'
        return 'VARCHAR(255)' # Default for general String

    # 2. Create Table (Dynamic Create)
    col_defs = [f"{col} {get_sql_type(col, first_row[col])}" for col in columns]
    
    # Define Primary Key (assuming period and currency_code are always primary keys)
    pk_cols = [c for c in ['period', 'currency_code'] if c in columns]
    pk_sql = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

    create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({', '.join(col_defs)}{pk_sql});"
    cursor.execute(create_sql)

    # 3. Check and add missing Columns (Schema Evolution)
    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    existing_columns = {row[0] for row in cursor.fetchall()}
    
    for col in columns:
        if col not in existing_columns:
            alter_sql = f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} {get_sql_type(col, first_row[col])}"
            cursor.execute(alter_sql)

    # 4. Create Insert statement (Dynamic Upsert)
    cols_str = ", ".join(columns)
    vals_str = ", ".join(["%s"] * len(columns))
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns if col not in pk_cols])
    
    sql = f"INSERT INTO {TABLE_NAME} ({cols_str}) VALUES ({vals_str}) ON DUPLICATE KEY UPDATE {update_clause}"
    
    # Prepare data and Execute in Batch
    # Convert to list of tuples for safer execution with some drivers
    values = [tuple(row[col] for col in columns) for row in data]
    
    logging.info(f"Executing Batch Insert into table '{TABLE_NAME}' in database '{current_db}'...")
    cursor.executemany(sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"✅ Successfully loaded {len(data)} rows into {current_db}.{TABLE_NAME}")

# --- DAG Definition ---
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
    "email": ["bot_exchange_rate@email.com"], # <--- 
    "email_on_failure": True,            # <--- Enable email alerts on failure
    "retries": 1,
    # "on_failure_callback": slack_alert, # <--- Slack alert on failure
    "on_failure_callback": discord_alert, # <--- Discord alert on failure
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bot_exchange_rate_pipeline",
    schedule="0 13 * * 1-5", # Run every Monday-Friday at 13:00 (BOT data usually arrives in the afternoon)
    catchup=False,
    default_args=default_args,
    tags=["bot", "finance", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_bot_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_bot_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_bot_data",
        python_callable=load_data,
    )

    # Task to send email on successful completion
    email_success_task = EmailOperator(
        task_id="send_email_success",
        to="bot_exchange_rate@email.com", # <--- Change to your email
        subject="✅ BOT Exchange Rate Pipeline Success ({{ ti.xcom_pull(task_ids='extract_bot_data', key='data_date') }})",
        html_content="<h3>Pipeline Completed Successfully</h3><p>Data has been loaded into MySQL. Please find the attached CSV report.</p>",
        files=[f"{CSV_REPORT_BASE_PATH}_{{{{ ti.xcom_pull(task_ids='extract_bot_data', key='data_date') }}}}.csv"]
    )

    extract_task >> transform_task >> load_task >> email_success_task