# Pipeline สำหรับดึงข้อมูลอัตราแลกเปลี่ยนจากธนาคารแห่งประเทศไทย (BOT)
# ETL Process:
# 1. Extract: ดึงข้อมูล Daily Average Exchange Rate จาก BOT API
# 2. Transform: เลือกเฉพาะสกุลเงินที่สนใจ (USD, EUR, JPY, etc.) และแปลง Type ข้อมูล
# 3. Load: บันทึกลง MySQL ตาราง bot_exchange_rates

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import requests
import logging

# --- Configuration ---
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "bot_exchange_rates"

# BOT API Configuration
# สมัคร API Key ได้ที่: https://portal.api.bot.or.th/ (New Portal)
# หมายเหตุ: ใช้ค่า API Key จาก Developer Portal ใหม่
API_URL = "https://gateway.api.bot.or.th/Stat-ExchangeRate/v2/DAILY_AVG_EXG_RATE/"

def extract_data(**context):
    """
    Extract: ดึงข้อมูลจาก BOT API ตามวันที่ run (execution_date)
    """
    # ดึง Key ภายใน function เพื่อลด load ของ Airflow Scheduler และป้องกัน error หากยังไม่สร้าง Variable
    api_key = Variable.get("bot_api_key", default_var="bot_api_key_default")

    # ใช้ data_interval_end หรือ ds (วันที่รัน) เพื่อดึงข้อมูลของวันนั้นๆ
    # หมายเหตุ: ข้อมูล BOT มักจะมาช้ากว่าปัจจุบันเล็กน้อย อาจต้องปรับ start_period ย้อนหลังถ้าจำเป็น
    execution_date = context['ds'] 
    
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
    TARGET_CURRENCIES = ["USD", "EUR", "JPY", "GBP", "SGD", "CNY"]
    
    transformed_data = []
    for item in raw_data:
        currency_code = item.get("currency_id")
        
        # Filter เฉพาะสกุลเงินที่สนใจ
        if currency_code in TARGET_CURRENCIES:
            transformed_data.append({
                "period": item.get("period"),
                "currency_code": currency_code,
                "currency_name": item.get("currency_name_th"),
                "buying_rate": float(item.get("buying_transfer", 0) or 0), # ใช้ buying_transfer เป็นหลัก
                "selling_rate": float(item.get("selling", 0) or 0),
                "mid_rate": float(item.get("mid_rate", 0) or 0),
                "updated_at": datetime.now().isoformat()
            })
            
    logging.info(f"Transformed data: {len(transformed_data)} records")
    context["ti"].xcom_push(key="cleaned_bot_data", value=transformed_data)

def load_data(**context):
    """
    Load: บันทึกลง MySQL (Upsert)
    """
    data = context["ti"].xcom_pull(key="cleaned_bot_data")
    if not data:
        logging.info("No data to load")
        return

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # สร้างตารางถ้ายังไม่มี
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            period DATE,
            currency_code VARCHAR(10),
            currency_name VARCHAR(100),
            buying_rate DECIMAL(10,4),
            selling_rate DECIMAL(10,4),
            mid_rate DECIMAL(10,4),
            updated_at TIMESTAMP,
            PRIMARY KEY (period, currency_code)
        );
    """)

    # Insert หรือ Update ข้อมูล (Upsert)
    sql = f"""
        INSERT INTO {TABLE_NAME} (period, currency_code, currency_name, buying_rate, selling_rate, mid_rate, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        buying_rate=VALUES(buying_rate), selling_rate=VALUES(selling_rate), 
        mid_rate=VALUES(mid_rate), updated_at=VALUES(updated_at)
    """

    for row in data:
        cursor.execute(sql, (
            row["period"], row["currency_code"], row["currency_name"],
            row["buying_rate"], row["selling_rate"], row["mid_rate"], row["updated_at"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Loaded {len(data)} rows into MySQL")

# --- DAG Definition ---
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
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

    extract_task >> transform_task >> load_task