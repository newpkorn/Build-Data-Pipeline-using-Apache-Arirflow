from datetime import datetime, timedelta
import os
from typing import Any, Dict

import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from utils.alerting import discord_alert, discord_success_alert


MYSQL_CONN_ID = "mysql_default"
TABLE_NAME = "weather_observations"
API_URL = "https://api.openweathermap.org/data/2.5/weather"
local_tz = pendulum.timezone("Asia/Bangkok")


def _get_variable(name: str, env_name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(env_name, default))


def _build_weather_html(record: Dict[str, Any]) -> str:
    return f"""
    <h3>Weather Pipeline Completed Successfully</h3>
    <p>The latest weather data has been loaded into MySQL.</p>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th align="left">City</th><td>{record['city']}</td></tr>
      <tr><th align="left">Description</th><td>{record['weather_description']}</td></tr>
      <tr><th align="left">Temperature (C)</th><td>{record['temperature_celsius']}</td></tr>
      <tr><th align="left">Feels Like (C)</th><td>{record['feels_like_celsius']}</td></tr>
      <tr><th align="left">Humidity</th><td>{record['humidity']}</td></tr>
      <tr><th align="left">Wind Speed</th><td>{record['wind_speed']}</td></tr>
      <tr><th align="left">Observed At</th><td>{record['observed_at_local']}</td></tr>
    </table>
    """


def extract_weather_data(**context) -> Dict[str, Any]:
    api_key = _get_variable("openweather_api_key", "OPENWEATHER_API_KEY")
    city = _get_variable("weather_city", "WEATHER_CITY", "Portland")

    if not api_key:
        raise ValueError(
            "OpenWeather API key is not configured. Set Airflow Variable "
            "'openweather_api_key' or environment variable 'OPENWEATHER_API_KEY'."
        )

    response = requests.get(
        API_URL,
        params={"q": city, "appid": api_key, "units": "metric"},
        timeout=30,
    )
    response.raise_for_status()

    payload = response.json()
    context["ti"].xcom_push(key="weather_city", value=city)
    context["ti"].xcom_push(key="raw_weather_payload", value=payload)
    return payload


def transform_weather_data(**context) -> Dict[str, Any]:
    data = context["ti"].xcom_pull(task_ids="extract_weather_data")
    if not data:
        raise ValueError("No weather payload found in XCom from extract_weather_data.")

    city = data["name"]
    observed_at = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
    sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
    sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

    transformed_data = {
        "city": city,
        "weather_description": data["weather"][0]["description"],
        "temperature_celsius": round(float(data["main"]["temp"]), 2),
        "feels_like_celsius": round(float(data["main"]["feels_like"]), 2),
        "minimum_temp_celsius": round(float(data["main"]["temp_min"]), 2),
        "maximum_temp_celsius": round(float(data["main"]["temp_max"]), 2),
        "pressure": int(data["main"]["pressure"]),
        "humidity": int(data["main"]["humidity"]),
        "wind_speed": float(data["wind"]["speed"]),
        "observed_at_local": observed_at.strftime("%Y-%m-%d %H:%M:%S"),
        "sunrise_local": sunrise_time.strftime("%Y-%m-%d %H:%M:%S"),
        "sunset_local": sunset_time.strftime("%Y-%m-%d %H:%M:%S"),
        "created_at": pendulum.now(local_tz).strftime("%Y-%m-%d %H:%M:%S"),
    }

    context["ti"].xcom_push(key="weather_record", value=transformed_data)
    return transformed_data


def load_weather_data(**context) -> None:
    weather_record = context["ti"].xcom_pull(
        task_ids="transform_weather_data",
        key="weather_record",
    )
    if not weather_record:
        raise ValueError("No transformed weather record found in XCom.")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            city VARCHAR(100) NOT NULL,
            weather_description VARCHAR(255),
            temperature_celsius DECIMAL(8,2),
            feels_like_celsius DECIMAL(8,2),
            minimum_temp_celsius DECIMAL(8,2),
            maximum_temp_celsius DECIMAL(8,2),
            pressure INT,
            humidity INT,
            wind_speed DECIMAL(8,2),
            observed_at_local DATETIME NOT NULL,
            sunrise_local DATETIME,
            sunset_local DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_weather_city_observed (city, observed_at_local)
        )
    """
    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            city,
            weather_description,
            temperature_celsius,
            feels_like_celsius,
            minimum_temp_celsius,
            maximum_temp_celsius,
            pressure,
            humidity,
            wind_speed,
            observed_at_local,
            sunrise_local,
            sunset_local,
            created_at
        ) VALUES (
            %(city)s,
            %(weather_description)s,
            %(temperature_celsius)s,
            %(feels_like_celsius)s,
            %(minimum_temp_celsius)s,
            %(maximum_temp_celsius)s,
            %(pressure)s,
            %(humidity)s,
            %(wind_speed)s,
            %(observed_at_local)s,
            %(sunrise_local)s,
            %(sunset_local)s,
            %(created_at)s
        )
        ON DUPLICATE KEY UPDATE
            weather_description = VALUES(weather_description),
            temperature_celsius = VALUES(temperature_celsius),
            feels_like_celsius = VALUES(feels_like_celsius),
            minimum_temp_celsius = VALUES(minimum_temp_celsius),
            maximum_temp_celsius = VALUES(maximum_temp_celsius),
            pressure = VALUES(pressure),
            humidity = VALUES(humidity),
            wind_speed = VALUES(wind_speed),
            sunrise_local = VALUES(sunrise_local),
            sunset_local = VALUES(sunset_local),
            created_at = VALUES(created_at)
    """
    mysql_hook.run(create_table_sql)
    mysql_hook.run(insert_sql, parameters=weather_record)


def prepare_weather_email(**context) -> None:
    weather_record = context["ti"].xcom_pull(
        task_ids="transform_weather_data",
        key="weather_record",
    )
    if not weather_record:
        raise ValueError("No transformed weather record found for email preparation.")

    city_slug = weather_record["city"].lower().replace(" ", "_")
    observed_at_slug = weather_record["observed_at_local"].replace(" ", "_").replace(":", "-")
    csv_file_path = f"/tmp/weather_{city_slug}_{observed_at_slug}.csv"

    pd.DataFrame([weather_record]).to_csv(csv_file_path, index=False)

    context["ti"].xcom_push(key="report_file", value=csv_file_path)
    context["ti"].xcom_push(key="html_report", value=_build_weather_html(weather_record))
    context["ti"].xcom_push(key="report_subject_date", value=weather_record["observed_at_local"])


ALERT_EMAIL = os.getenv("WEATHER_ALERT_EMAIL", "airflow@example.com")

default_args = {
    "owner": "dataeng",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": discord_alert,
}


with DAG(
    dag_id="weather_api_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    on_success_callback=discord_success_alert,
    tags=["weather", "api", "mysql", "notification"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
    )

    prepare_email_task = PythonOperator(
        task_id="prepare_weather_email",
        python_callable=prepare_weather_email,
    )

    send_success_email_task = EmailOperator(
        task_id="send_weather_success_email",
        to=ALERT_EMAIL,
        subject="Weather Pipeline Success {{ ti.xcom_pull(task_ids='prepare_weather_email', key='report_subject_date') }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare_weather_email', key='html_report') }}",
        files=["{{ ti.xcom_pull(task_ids='prepare_weather_email', key='report_file') }}"],
    )

    extract_task >> transform_task >> load_task >> prepare_email_task >> send_success_email_task
