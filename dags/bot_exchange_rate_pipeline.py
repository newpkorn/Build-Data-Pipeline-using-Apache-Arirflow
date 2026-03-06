# Pipeline สำหรับดึงข้อมูลอัตราแลกเปลี่ยนจากธนาคารแห่งประเทศไทย (BOT)
# ETL Process:
# 1. Extract: ดึงข้อมูล Daily Average Exchange Rate จาก BOT API
# 2. Transform: เลือกเฉพาะสกุลเงินที่สนใจ (USD, EUR, JPY, etc.) และแปลง Type ข้อมูล
# 3. Load: บันทึกลง MySQL ตาราง bot_exchange_rates

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import requests
import logging
import csv

# --- Configuration ---
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "bot_exchange_rates"

# BOT API Configuration
# สมัคร API Key ได้ที่: https://portal.api.bot.or.th/ (New Portal)
# หมายเหตุ: ใช้ค่า API Key จาก Developer Portal ใหม่
API_URL = "https://gateway.api.bot.or.th/Stat-ExchangeRate/v2/DAILY_AVG_EXG_RATE/"

# Base Path สำหรับไฟล์ CSV (จะมีการเติมวันที่เข้าไปตอนรัน เช่น _2026-03-06.csv)
CSV_REPORT_BASE_PATH = "/tmp/bot_exchange_rates"

def extract_data(**context):
    """
    Extract: ดึงข้อมูลจาก BOT API ตามวันที่ run (execution_date)
    """
    # ดึง Key ภายใน function เพื่อลด load ของ Airflow Scheduler และป้องกัน error หากยังไม่สร้าง Variable
    api_key = Variable.get("bot_api_key", default_var="bot_api_key_default")

    # ใช้ data_interval_end หรือ ds (วันที่รัน) เพื่อดึงข้อมูลของวันนั้นๆ
    # หมายเหตุ: ข้อมูล BOT มักจะมาช้ากว่าปัจจุบันเล็กน้อย อาจต้องปรับ start_period ย้อนหลังถ้าจำเป็น
    execution_date = context['ds'] 
    # execution_date = context['yesterday_ds']
    
    logging.info(f"Fetching BOT data for date: {execution_date}")
    
    headers = {
        "Authorization": api_key,
        "Accept": "application/json"
    }
    
    params = {
        "start_period": execution_date,
        "end_period": execution_date
    }

    response = requests.get(API_URL, headers=headers, params=params, timeout=30)
    
    # ตรวจสอบ Error
    if response.status_code != 200:
        logging.error(f"API Error: {response.text}")
        # ถ้าเป็น 404 หรือไม่มีข้อมูล อาจจะยอมให้ผ่านไปได้ หรือ raise error ตาม business logic
        response.raise_for_status()
        
    data = response.json()
    
    # ตรวจสอบโครงสร้าง Response ของ BOT
    if "result" in data and "data" in data["result"] and "data_detail" in data["result"]["data"]:
        raw_data = data["result"]["data"]["data_detail"]
        logging.info(f"Extracted {len(raw_data)} records.")
        context["ti"].xcom_push(key="raw_bot_data", value=raw_data)
    else:
        logging.warning("No data found in BOT response")
        context["ti"].xcom_push(key="raw_bot_data", value=[])

def transform_data(**context):
    """
    Transform: คัดกรองสกุลเงินและแปลงข้อมูล
    """
    raw_data = context["ti"].xcom_pull(key="raw_bot_data")
    if not raw_data:
        logging.info("No data to transform")
        return []

    # สกุลเงินที่ต้องการ (ถ้าต้องการทั้งหมดให้ลบ filter นี้ออก)
    # TARGET_CURRENCIES = ["USD", "EUR", "JPY", "GBP", "SGD", "CNY"]
    
    transformed_data = []
    for item in raw_data:
        currency_code = item.get("currency_id")
        period = item.get("period")

        # Data Cleaning: ข้ามข้อมูลที่ไม่มีวันที่ (Fix Error 1292) หรือไม่มี Currency Code
        if not period or not currency_code:
            continue
        
        # Filter เฉพาะสกุลเงินที่สนใจ
        # if currency_code in TARGET_CURRENCIES:  <-- Comment แค่เงื่อนไข
        transformed_data.append({               # <-- เปิดส่วนนี้ และขยับ Indent ให้ตรง
            "period": period,
            "currency_code": currency_code,
            "currency_name": item.get("currency_name_th"),
            "buying_rate": float(item.get("buying_transfer", 0) or 0), # ใช้ buying_transfer เป็นหลัก
            "selling_rate": float(item.get("selling", 0) or 0),
            "mid_rate": float(item.get("mid_rate", 0) or 0),
            "updated_at": datetime.now().isoformat()
        })
            
    logging.info(f"Transformed data: {len(transformed_data)} records")

    # สร้างชื่อไฟล์ตามวันที่ (Dynamic Filename)
    csv_file_path = f"{CSV_REPORT_BASE_PATH}_{context['ds']}.csv"
    
    # สร้างไฟล์ CSV สำหรับแนบอีเมล
    if transformed_data:
        # กรณีมีข้อมูล: ใช้ Keys จากข้อมูลจริง (Dynamic) ไม่ต้อง Hardcode
        fieldnames = transformed_data[0].keys()
        with open(csv_file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transformed_data)
    else:
        # กรณีไม่มีข้อมูล: สร้างไฟล์ Text ธรรมดาแจ้งว่าไม่มีข้อมูล (ป้องกัน FileNotFoundError)
        with open(csv_file_path, "w", encoding="utf-8") as f:
            f.write("No data available for this date.")
            
    context["ti"].xcom_push(key="cleaned_bot_data", value=transformed_data)

def load_data(**context):
    """
    Load: บันทึกลง MySQL แบบ Dynamic Schema
    (สร้าง Column ตาม Keys ของข้อมูลจริง ไม่ Hardcode)
    """
    data = context["ti"].xcom_pull(key="cleaned_bot_data")
    if not data:
        logging.info("No data to load")
        return

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Self-Healing: ตรวจสอบว่าตารางมี Primary Key หรือไม่ (ป้องกันข้อมูลซ้ำจากตารางเวอร์ชันเก่า)
    cursor.execute(f"SHOW TABLES LIKE '{TABLE_NAME}'")
    if cursor.fetchone():
        cursor.execute(f"SHOW KEYS FROM {TABLE_NAME} WHERE Key_name = 'PRIMARY'")
        if not cursor.fetchone():
            logging.warning(f"Table {TABLE_NAME} exists but missing Primary Key. Dropping to recreate with correct schema.")
            cursor.execute(f"DROP TABLE {TABLE_NAME}")

    # 1. วิเคราะห์ข้อมูลเพื่อสร้าง Schema
    first_row = data[0]
    columns = list(first_row.keys())

    # Helper: Map Python Type -> MySQL Type
    def get_sql_type(col_name, value):
        if col_name == 'period': return 'DATE'
        if 'rate' in col_name: return 'DECIMAL(10,4)' # ดักจับ buying_rate, selling_rate
        if 'updated_at' in col_name: return 'TIMESTAMP'
        if isinstance(value, int): return 'INT'
        if isinstance(value, float): return 'DECIMAL(10,4)'
        return 'VARCHAR(255)' # Default สำหรับ String ทั่วไป

    # 2. สร้างตาราง (Dynamic Create)
    col_defs = [f"{col} {get_sql_type(col, first_row[col])}" for col in columns]
    
    # กำหนด Primary Key (สมมติว่า period กับ currency_code เป็น key หลักเสมอ)
    pk_cols = [c for c in ['period', 'currency_code'] if c in columns]
    pk_sql = f", PRIMARY KEY ({', '.join(pk_cols)})" if pk_cols else ""

    create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({', '.join(col_defs)}{pk_sql});"
    cursor.execute(create_sql)

    # 3. ตรวจสอบและเพิ่ม Column ที่ขาดหาย (Schema Evolution)
    cursor.execute(f"DESCRIBE {TABLE_NAME}")
    existing_columns = {row[0] for row in cursor.fetchall()}
    
    for col in columns:
        if col not in existing_columns:
            alter_sql = f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col} {get_sql_type(col, first_row[col])}"
            cursor.execute(alter_sql)

    # 4. สร้างคำสั่ง Insert (Dynamic Upsert)
    cols_str = ", ".join(columns)
    vals_str = ", ".join(["%s"] * len(columns))
    update_clause = ", ".join([f"{col}=VALUES({col})" for col in columns if col not in pk_cols])
    
    sql = f"INSERT INTO {TABLE_NAME} ({cols_str}) VALUES ({vals_str}) ON DUPLICATE KEY UPDATE {update_clause}"
    
    # เตรียมข้อมูลและ Execute แบบ Batch
    values = [[row[col] for col in columns] for row in data]
    cursor.executemany(sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Loaded {len(data)} rows into MySQL")

# --- DAG Definition ---
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
    "email": ["bot_exchange_rate@email.com"], # <--- 
    "email_on_failure": True,            # <--- เปิดระบบแจ้งเตือนเมื่อพัง
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bot_exchange_rate_pipeline",
    schedule_interval="0 13 * * 1-5", # รันทุกวันจันทร์-ศุกร์ เวลา 13:00 (ข้อมูล BOT มักจะมาช่วงบ่าย)
    catchup=False,
    default_args=default_args,
    tags=["bot", "finance", "etl"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_bot_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_bot_data",
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_bot_data",
        python_callable=load_data,
        provide_context=True
    )

    # Task ส่งอีเมลเมื่อทำงานสำเร็จ
    email_success_task = EmailOperator(
        task_id="send_email_success",
        to="bot_exchange_rate@email.com", # <--- แก้เป็นอีเมลของคุณ
        subject="✅ BOT Exchange Rate Pipeline Success ({{ ds }})",
        html_content="<h3>Pipeline Completed Successfully</h3><p>Data has been loaded into MySQL. Please find the attached CSV report.</p>",
        files=[f"{CSV_REPORT_BASE_PATH}_{{{{ ds }}}}.csv"]
    )

    extract_task >> transform_task >> load_task >> email_success_task