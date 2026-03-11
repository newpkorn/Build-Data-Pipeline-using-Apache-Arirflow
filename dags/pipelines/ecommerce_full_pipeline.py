# สร้างไฟล์ใหม่ให้แล้วครับ! ผมได้สร้างไฟล์ ecommerce_full_pipeline.py ในโฟลเดอร์ dags ซึ่งเป็นตัวอย่าง full-loop data pipeline แบบ end-to-end ที่ใช้ในธุรกิจจริง (e-commerce use case)

# ภาพรวม Pipeline (Full Loop):
# 1. Extract: ดึงข้อมูลคำสั่งซื้อจาก API (ใช้ Fake Store API เป็นตัวอย่าง)
# 2. Transform:
# - Clean ข้อมูล (ลบราคาที่ไม่ถูกต้อง)
# - Aggregate ข้อมูล (รวมยอดขายตาม category)
# 3. Load:
# - Load ข้อมูล orders เข้า MySQL ตาราง orders
# - Load ข้อมูล aggregated เข้า MySQL ตาราง daily_reports
# 4. Generate Report: สร้าง CSV และ HTML report
# 5. Send Email: ส่งรายงานทางอีเมล
# Use Case จริง:
# - ธุรกิจ: E-commerce platform ที่ต้องการรายงานยอดขายประจำวัน
# - ประโยชน์: ช่วยวิเคราะห์แนวโน้มการขาย, ตัดสินใจ inventory, และส่งรายงานอัตโนมัติ
# - Schedule: รันทุกวัน (@daily)
# Workflow ใน Airflow:
#  extract_orders → transform_orders → [load_orders, load_reports] → generate_report → send_email_report
# การใช้งาน:
# 1. ตรวจสอบ MySQL connection ใน Airflow (ชื่อ mysql_default)
# 2. รัน DAG ใน Airflow UI
# 3. ดู logs และ reports ที่ส่งมาทางอีเมล
# นี่คือตัวอย่างที่ครอบคลุมทั้ง ETL, reporting, และ automation ถ้าต้องการปรับแต่ง (เช่น เปลี่ยน data source หรือเพิ่ม features) บอกได้เลยครับ!

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import requests
import os

# Configuration
MYSQL_CONN_ID = "mysql_default"
ORDERS_TABLE = "orders"
REPORTS_TABLE = "daily_reports"
API_URL = "https://dummyjson.com/products"  # Real e-commerce API with prices
TEMP_REPORT_PATH = "/tmp/daily_sales_report.csv"

def extract_orders(**context):
    """Extract: Fetch real product orders data from DummyJSON API"""
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    api_response = response.json()
    
    # DummyJSON returns {"products": [...]}
    data = api_response.get("products", [])

    # Transform products data into orders format
    orders = []
    
    for item in data[:10]:  # Limit to 10 products for demo
        orders.append({
            "order_id": item.get("id"),
            "product_name": item.get("title", "Unknown"),
            "price": item.get("price", 0),  # Real price from API!
            "category": item.get("category", "unknown"),  # Real category!
            "order_date": datetime.now().date().isoformat(),
            "quantity": 1  # Simulated quantity
        })

    context["ti"].xcom_push(key="orders_data", value=orders)
    return orders

def transform_orders(**context):
    """Transform: Clean and aggregate data"""
    orders = context["ti"].xcom_pull(key="orders_data")

    # ensure we always work with a list
    if orders is None:
        # This can happen if extract failed or returned no data
        orders = []
        import logging
        logging.warning(
            "transform_orders received no orders_data (None); "
            "defaulting to empty list"
        )

    # Clean: Remove invalid prices
    cleaned_orders = [order for order in orders if order.get("price", 0) > 0]

    # Aggregate: Calculate total sales per category
    if not cleaned_orders:
        # No data to aggregate
        import logging
        logging.warning("No valid orders to aggregate (all filtered out or empty)")
        aggregated_list = []
    else:
        df = pd.DataFrame(cleaned_orders)
        df["total"] = df["price"] * df["quantity"]
        aggregated = df.groupby("category").agg({
            "total": "sum",
            "quantity": "sum"
        }).reset_index()
        aggregated_list = aggregated.to_dict("records")

    # Add metadata
    for item in aggregated_list:
        item["report_date"] = datetime.now().date().isoformat()
        item["processed_at"] = datetime.now().isoformat()

    context["ti"].xcom_push(key="aggregated_data", value=aggregated_list)
    return aggregated_list

def load_orders(**context):
    """Load: Insert orders into MySQL"""
    orders = context["ti"].xcom_pull(key="orders_data")
    
    # handle None
    if orders is None:
        orders = []
    
    if not orders:
        import logging
        logging.info("No orders to load")
        return

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Create orders table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
            order_id INT PRIMARY KEY,
            product_name VARCHAR(255),
            price DECIMAL(10,2),
            category VARCHAR(100),
            order_date DATE,
            quantity INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Insert orders
    for order in orders:
        cursor.execute(f"""
            INSERT INTO {ORDERS_TABLE} (order_id, product_name, price, category, order_date, quantity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            product_name=VALUES(product_name), price=VALUES(price)
        """, (order.get("order_id"), order.get("product_name"), order.get("price"),
              order.get("category"), order.get("order_date"), order.get("quantity")))

    conn.commit()
    cursor.close()
    conn.close()

def load_reports(**context):
    """Load: Insert aggregated reports into MySQL"""
    reports = context["ti"].xcom_pull(key="aggregated_data")
    
    # handle None
    if reports is None:
        reports = []
    
    if not reports:
        import logging
        logging.info("No reports to load")
        return

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    # Create reports table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {REPORTS_TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(100),
            total_sales DECIMAL(10,2),
            total_quantity INT,
            report_date DATE,
            processed_at TIMESTAMP
        );
    """)

    # Insert reports
    for report in reports:
        cursor.execute(f"""
            INSERT INTO {REPORTS_TABLE} (category, total_sales, total_quantity, report_date, processed_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (report.get("category"), report.get("total"), report.get("quantity"),
              report.get("report_date"), report.get("processed_at")))

    conn.commit()
    cursor.close()
    conn.close()

def generate_report(**context):
    """Generate: Create CSV report and HTML summary"""
    reports = context["ti"].xcom_pull(key="aggregated_data")
    
    # handle None or empty
    if not reports:
        reports = []
    
    df = pd.DataFrame(reports)

    # Save CSV
    df.to_csv(TEMP_REPORT_PATH, index=False)

    # Create HTML summary
    if df.empty:
        total_sales = 0
        total_orders = 0
        table_rows = "<tr><td colspan='3'>No data available</td></tr>"
    else:
        total_sales = df["total"].sum() if "total" in df.columns else 0
        total_orders = df["quantity"].sum() if "quantity" in df.columns else 0
        table_rows = "".join(
            f"<tr><td>{row.get('category', 'N/A')}</td>"
            f"<td>${row.get('total', 0):.2f}</td>"
            f"<td>{row.get('quantity', 0)}</td></tr>"
            for _, row in df.iterrows()
        )

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><title>Daily Sales Report</title></head>
    <body>
        <h2>📊 Daily E-Commerce Sales Report</h2>
        <p><strong>Report Date:</strong> {datetime.now().date()}</p>
        <p><strong>Total Sales:</strong> ${total_sales:.2f}</p>
        <p><strong>Total Orders:</strong> {total_orders}</p>
        <h3>Sales by Category:</h3>
        <table border="1">
            <tr><th>Category</th><th>Total Sales</th><th>Quantity</th></tr>
            {table_rows}
        </table>
    </body>
    </html>
    """

    context["ti"].xcom_push(key="html_report", value=html_content)
    return html_content

# DAG Configuration
default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
    "email": ["reports@company.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_full_pipeline",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "etl", "reporting"]
) as dag:

    # ETL Tasks
    extract = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    transform = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders,
    )

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    load_reports = PythonOperator(
        task_id="load_reports",
        python_callable=load_reports,
    )

    # Reporting Tasks
    generate = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    send_email = EmailOperator(
        task_id="send_email_report",
        to="reports@company.com",
        subject="📊 Daily E-Commerce Sales Report",
        html_content="{{ ti.xcom_pull(key='html_report') }}",
        files=[TEMP_REPORT_PATH],
        mime_charset='utf-8',
    )

    # Workflow: Extract -> Transform -> Load -> Generate Report -> Send Email
    extract >> transform >> [load_orders, load_reports] >> generate >> send_email
