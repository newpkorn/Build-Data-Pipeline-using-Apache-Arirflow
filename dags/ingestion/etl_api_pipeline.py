# สร้างไฟล์ใหม่ให้แล้วครับ! ผมได้สร้างไฟล์ etl_api_pipeline.py ในโฟลเดอร์ dags พร้อมเขียนโค้ด ETL pipeline ตามที่คุณระบุ (ใช้ API เป็น data source และมี data cleaning เป็น transformation)

# สรุป pipeline นี้:
# Extract: ดึงข้อมูลจาก API https://jsonplaceholder.typicode.com/posts (ตัวอย่าง API ที่ให้ข้อมูล posts)
# Transform:
# ลบข้อมูลที่ไม่มี userId, title, หรือ body
# เพิ่มฟิลด์ processed_at เป็น timestamp ปัจจุบัน
# Load: Insert ข้อมูลเข้า MySQL ตาราง api_data ด้วย upsert (อัปเดตถ้าซ้ำ)
# การใช้งาน:
# ตรวจสอบว่า MySQL connection ใน Airflow มีชื่อ mysql_default (หรือปรับในโค้ด)
# รัน DAG ใน Airflow UI หรือด้วย command line
# Pipeline จะรันทุกวัน (@daily) หรือ trigger manually

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
import requests
import json

# Configuration
MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "api_data"
API_URL = "https://jsonplaceholder.typicode.com/posts"  # Example API

def extract_data(**context):
    """Extract data from API"""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    context["ti"].xcom_push(key="raw_data", value=data)
    return data

def transform_data(**context):
    """Transform: Clean and filter data"""
    raw_data = context["ti"].xcom_pull(key="raw_data")
    # Example transformations: Remove null values, filter by userId
    cleaned_data = [
        item for item in raw_data
        if item.get("userId") and item.get("title") and item.get("body")
    ]
    # Add processed timestamp
    for item in cleaned_data:
        item["processed_at"] = datetime.now().isoformat()
    context["ti"].xcom_push(key="cleaned_data", value=cleaned_data)
    return cleaned_data

def load_data(**context):
    """Load data into MySQL"""
    cleaned_data = context["ti"].xcom_pull(key="cleaned_data")
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT PRIMARY KEY,
        userId INT,
        title VARCHAR(255),
        body TEXT,
        processed_at TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)

    # Insert data
    for item in cleaned_data:
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (id, userId, title, body, processed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title=VALUES(title), body=VALUES(body), processed_at=VALUES(processed_at)
        """, (item["id"], item["userId"], item["title"], item["body"], item["processed_at"]))

    conn.commit()
    cursor.close()
    conn.close()

# DAG definition
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_api_pipeline",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "api"]
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load
