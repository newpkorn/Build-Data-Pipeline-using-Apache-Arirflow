import requests
import csv
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.hooks.mysql_hook import MySqlHook


MYSQL_CONN_ID = "covid19"
TABLE_NAME = "covid19_global_auto"
TEMP_CSV_PATH = "/tmp/covid_report.csv"


# ==============================
# 1️⃣ FETCH + LOAD (Dynamic + Safe)
# ==============================
def fetch_and_load(**context):

    url = "https://disease.sh/v3/covid-19/all"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    def map_type(value):
        if isinstance(value, int):
            return "BIGINT"
        elif isinstance(value, float):
            return "DOUBLE"
        elif isinstance(value, bool):
            return "BOOLEAN"
        else:
            return "TEXT"

    # ---- Create Table ----
    columns_sql = [
        f"`{k}` {map_type(v)}"
        for k, v in data.items()
    ]

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {', '.join(columns_sql)},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_updated (`updated`)
    );
    """
    cursor.execute(create_sql)

    # ---- Auto Add Column ----
    cursor.execute(f"SHOW COLUMNS FROM `{TABLE_NAME}`;")
    existing_columns = {row[0] for row in cursor.fetchall()}

    for k, v in data.items():
        if k not in existing_columns:
            cursor.execute(
                f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN `{k}` {map_type(v)};"
            )

    # ---- Insert (Idempotent) ----
    keys = list(data.keys())
    columns = ", ".join([f"`{k}`" for k in keys])
    placeholders = ", ".join(["%s"] * len(keys))

    insert_sql = f"""
        INSERT INTO `{TABLE_NAME}` ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {', '.join([f"`{k}` = VALUES(`{k}`)" for k in keys])};
    """

    cursor.execute(insert_sql, tuple(data.values()))
    conn.commit()

    # ถ้ามี new row จริง
    inserted = cursor.rowcount
    context["ti"].xcom_push(key="inserted", value=inserted)

    cursor.close()
    conn.close()


# ==============================
# 2️⃣ GENERATE REPORT
# ==============================
def generate_report(**context):

    inserted = context["ti"].xcom_pull(key="inserted")

    # Default values
    cases, deaths, recovered, updated = 0, 0, 0, 0
    
    if inserted > 0:
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        records = mysql_hook.get_first(
            f"SELECT cases, deaths, recovered, updated FROM `{TABLE_NAME}` ORDER BY updated DESC LIMIT 1;"
        )
        if records:
            cases, deaths, recovered, updated = records

    # ---- Generate CSV ----
    with open(TEMP_CSV_PATH, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Cases", "Deaths", "Recovered", "Updated"])
        if inserted > 0:
            writer.writerow([cases, deaths, recovered, updated])

    if inserted == 0:
        html_content = """<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body>
<h2>📊 COVID-19 Global Report</h2>
<p>⚠️ No new data updated in this run.</p>
</body>
</html>"""
    else:
        html_content = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body>
<h2>📊 COVID-19 Global Report</h2>
<ul>
    <li><b>Cases:</b> {cases:,}</li>
    <li><b>Deaths:</b> {deaths:,}</li>
    <li><b>Recovered:</b> {recovered:,}</li>
    <li><b>Last Update:</b> {datetime.fromtimestamp(updated/1000) if updated else 'N/A'}</li>
</ul>
</body>
</html>""".strip()

    context["ti"].xcom_push(key="html_report", value=html_content)
    return "READY"


# ==============================
# 3️⃣ AIRFLOW DAG
# ==============================

default_args = {
    "owner": "dataeng",
    "start_date": datetime(2024, 1, 1),
    "email": ["covid19-daily-report@email.com"],
    "email_on_failure": True,
}

with DAG(
    dag_id="covid19_enterprise_pipeline",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["enterprise", "dynamic-schema"]
) as dag:

    load = PythonOperator(
        task_id="fetch_and_load",
        python_callable=fetch_and_load,
        provide_context=True
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="covid19-daily-report@email.com",
        subject="📊 COVID19 Global Report",
        html_content="{{ ti.xcom_pull(key='html_report') }}",
        files=[TEMP_CSV_PATH],
        mime_charset='utf-8',
    )

    load >> report >> send_email