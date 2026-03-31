from datetime import datetime, timedelta
import os
from typing import Any, Dict, List

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

THAILAND_PROVINCES = [
    "Bangkok",
    "Krabi",
    "Kanchanaburi",
    "Kalasin",
    "Kamphaeng Phet",
    "Khon Kaen",
    "Chanthaburi",
    "Chachoengsao",
    "Chon Buri",
    "Chai Nat",
    "Chaiyaphum",
    "Chumphon",
    "Chiang Rai",
    "Chiang Mai",
    "Trang",
    "Trat",
    "Tak",
    "Nakhon Nayok",
    "Nakhon Pathom",
    "Nakhon Phanom",
    "Nakhon Ratchasima",
    "Nakhon Si Thammarat",
    "Nakhon Sawan",
    "Nonthaburi",
    "Narathiwat",
    "Nan",
    "Bueng Kan",
    "Buri Ram",
    "Pathum Thani",
    "Prachuap Khiri Khan",
    "Prachin Buri",
    "Pattani",
    "Phra Nakhon Si Ayutthaya",
    "Phayao",
    "Phang Nga",
    "Phatthalung",
    "Phichit",
    "Phitsanulok",
    "Phetchaburi",
    "Phetchabun",
    "Phrae",
    "Phuket",
    "Maha Sarakham",
    "Mukdahan",
    "Mae Hong Son",
    "Yasothon",
    "Yala",
    "Roi Et",
    "Ranong",
    "Rayong",
    "Ratchaburi",
    "Lop Buri",
    "Lampang",
    "Lamphun",
    "Loei",
    "Si Sa Ket",
    "Sakon Nakhon",
    "Songkhla",
    "Satun",
    "Samut Prakan",
    "Samut Songkhram",
    "Samut Sakhon",
    "Sa Kaeo",
    "Saraburi",
    "Sing Buri",
    "Sukhothai",
    "Suphan Buri",
    "Surat Thani",
    "Surin",
    "Nong Khai",
    "Nong Bua Lamphu",
    "Ang Thong",
    "Udon Thani",
    "Uthai Thani",
    "Uttaradit",
    "Ubon Ratchathani",
    "Amnat Charoen",
]

THAI_REGION_BY_PROVINCE = {
    "Bangkok": "Central",
    "Krabi": "South",
    "Kanchanaburi": "Central",
    "Kalasin": "Northeast",
    "Kamphaeng Phet": "North",
    "Khon Kaen": "Northeast",
    "Chanthaburi": "East",
    "Chachoengsao": "East",
    "Chon Buri": "East",
    "Chai Nat": "Central",
    "Chaiyaphum": "Northeast",
    "Chumphon": "South",
    "Chiang Rai": "North",
    "Chiang Mai": "North",
    "Trang": "South",
    "Trat": "East",
    "Tak": "West",
    "Nakhon Nayok": "Central",
    "Nakhon Pathom": "Central",
    "Nakhon Phanom": "Northeast",
    "Nakhon Ratchasima": "Northeast",
    "Nakhon Si Thammarat": "South",
    "Nakhon Sawan": "Central",
    "Nonthaburi": "Central",
    "Narathiwat": "South",
    "Nan": "North",
    "Bueng Kan": "Northeast",
    "Buri Ram": "Northeast",
    "Pathum Thani": "Central",
    "Prachuap Khiri Khan": "Central",
    "Prachin Buri": "East",
    "Pattani": "South",
    "Phra Nakhon Si Ayutthaya": "Central",
    "Phayao": "North",
    "Phang Nga": "South",
    "Phatthalung": "South",
    "Phichit": "North",
    "Phitsanulok": "North",
    "Phetchaburi": "Central",
    "Phetchabun": "North",
    "Phrae": "North",
    "Phuket": "South",
    "Maha Sarakham": "Northeast",
    "Mukdahan": "Northeast",
    "Mae Hong Son": "North",
    "Yasothon": "Northeast",
    "Yala": "South",
    "Roi Et": "Northeast",
    "Ranong": "South",
    "Rayong": "East",
    "Ratchaburi": "West",
    "Lop Buri": "Central",
    "Lampang": "North",
    "Lamphun": "North",
    "Loei": "Northeast",
    "Si Sa Ket": "Northeast",
    "Sakon Nakhon": "Northeast",
    "Songkhla": "South",
    "Satun": "South",
    "Samut Prakan": "Central",
    "Samut Songkhram": "Central",
    "Samut Sakhon": "Central",
    "Sa Kaeo": "East",
    "Saraburi": "Central",
    "Sing Buri": "Central",
    "Sukhothai": "North",
    "Suphan Buri": "Central",
    "Surat Thani": "South",
    "Surin": "Northeast",
    "Nong Khai": "Northeast",
    "Nong Bua Lamphu": "Northeast",
    "Ang Thong": "Central",
    "Udon Thani": "Northeast",
    "Uthai Thani": "Central",
    "Uttaradit": "North",
    "Ubon Ratchathani": "Northeast",
    "Amnat Charoen": "Northeast",
}


def _get_variable(name: str, env_name: str, default: str = "") -> str:
    return Variable.get(name, default_var=os.getenv(env_name, default))


def _get_weather_provinces() -> List[str]:
    configured = _get_variable("weather_cities", "WEATHER_CITIES", "")
    if configured.strip():
        provinces = [item.strip() for item in configured.split(",") if item.strip()]
        return provinces
    return THAILAND_PROVINCES


def _build_weather_html(summary: Dict[str, Any], preview_df: pd.DataFrame) -> str:
    hottest = summary["hottest_province"]
    coolest = summary["coolest_province"]
    avg_temp = summary["avg_temperature_celsius"]
    avg_humidity = summary["avg_humidity"]
    observed_at = summary["observed_at_local"]

    preview_rows = "".join(
        f"<tr><td>{row['province']}</td><td>{row['temperature_celsius']}</td><td>{row['humidity']}</td><td>{row['weather_description']}</td></tr>"
        for _, row in preview_df.iterrows()
    )

    return f"""
    <h3>Thailand Weather Pipeline Completed Successfully</h3>
    <p>The latest province-level weather data has been loaded into MySQL.</p>
    <ul>
      <li><strong>Observed at:</strong> {observed_at}</li>
      <li><strong>Tracked provinces:</strong> {summary['province_count']}</li>
      <li><strong>Failed provinces:</strong> {summary['failed_province_count']}</li>
      <li><strong>Average temperature:</strong> {avg_temp} C</li>
      <li><strong>Average humidity:</strong> {avg_humidity}%</li>
      <li><strong>Hottest province:</strong> {hottest}</li>
      <li><strong>Coolest province:</strong> {coolest}</li>
    </ul>
    <table border="1" cellpadding="6" cellspacing="0">
      <thead>
        <tr>
          <th>Province</th>
          <th>Temperature (C)</th>
          <th>Humidity</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {preview_rows}
      </tbody>
    </table>
    """


def extract_weather_data(**context) -> List[Dict[str, Any]]:
    api_key = _get_variable("openweather_api_key", "OPENWEATHER_API_KEY")
    provinces = _get_weather_provinces()

    if not api_key:
        raise ValueError(
            "OpenWeather API key is not configured. Set Airflow Variable "
            "'openweather_api_key' or environment variable 'OPENWEATHER_API_KEY'."
        )

    session = requests.Session()
    payloads: List[Dict[str, Any]] = []
    failed_provinces: List[str] = []

    for province in provinces:
        try:
            response = session.get(
                API_URL,
                params={"q": f"{province},TH", "appid": api_key, "units": "metric"},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            payload["_requested_province"] = province
            payloads.append(payload)
        except requests.RequestException:
            failed_provinces.append(province)

    if not payloads:
        raise ValueError("No province weather data could be fetched from OpenWeather.")

    context["ti"].xcom_push(key="weather_provinces", value=provinces)
    context["ti"].xcom_push(key="failed_weather_provinces", value=failed_provinces)
    context["ti"].xcom_push(key="raw_weather_payloads", value=payloads)
    return payloads


def transform_weather_data(**context) -> List[Dict[str, Any]]:
    payloads = context["ti"].xcom_pull(task_ids="extract_weather_data")
    if not payloads:
        raise ValueError("No weather payloads found in XCom from extract_weather_data.")

    transformed_records: List[Dict[str, Any]] = []
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        snapshot_date = data_interval_end.in_timezone(local_tz).to_date_string()
    else:
        snapshot_date = pendulum.now(local_tz).to_date_string()

    for data in payloads:
        observed_at = datetime.utcfromtimestamp(data["dt"] + data["timezone"])
        sunrise_time = datetime.utcfromtimestamp(data["sys"]["sunrise"] + data["timezone"])
        sunset_time = datetime.utcfromtimestamp(data["sys"]["sunset"] + data["timezone"])

        transformed_records.append(
            {
                "province": data["_requested_province"],
                "region": THAI_REGION_BY_PROVINCE.get(data["_requested_province"], "Other"),
                "city": data["name"],
                "country_code": data["sys"].get("country", "TH"),
                "weather_description": data["weather"][0]["description"],
                "temperature_celsius": round(float(data["main"]["temp"]), 2),
                "feels_like_celsius": round(float(data["main"]["feels_like"]), 2),
                "minimum_temp_celsius": round(float(data["main"]["temp_min"]), 2),
                "maximum_temp_celsius": round(float(data["main"]["temp_max"]), 2),
                "pressure": int(data["main"]["pressure"]),
                "humidity": int(data["main"]["humidity"]),
                "wind_speed": float(data["wind"]["speed"]),
                "observed_at_local": observed_at.strftime("%Y-%m-%d %H:%M:%S"),
                "observed_date": observed_at.strftime("%Y-%m-%d"),
                "sunrise_local": sunrise_time.strftime("%Y-%m-%d %H:%M:%S"),
                "sunset_local": sunset_time.strftime("%Y-%m-%d %H:%M:%S"),
                "created_at": pendulum.now(local_tz).strftime("%Y-%m-%d %H:%M:%S"),
                "snapshot_date": snapshot_date,
            }
        )

    context["ti"].xcom_push(key="weather_records", value=transformed_records)
    return transformed_records


def load_weather_data(**context) -> None:
    weather_records = context["ti"].xcom_pull(
        task_ids="transform_weather_data",
        key="weather_records",
    )
    if not weather_records:
        raise ValueError("No transformed weather records found in XCom.")

    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            province,
            region,
            city,
            country_code,
            weather_description,
            temperature_celsius,
            feels_like_celsius,
            minimum_temp_celsius,
            maximum_temp_celsius,
            pressure,
            humidity,
            wind_speed,
            observed_at_local,
            observed_date,
            sunrise_local,
            sunset_local,
            created_at,
            snapshot_date
        ) VALUES (
            %(province)s,
            %(region)s,
            %(city)s,
            %(country_code)s,
            %(weather_description)s,
            %(temperature_celsius)s,
            %(feels_like_celsius)s,
            %(minimum_temp_celsius)s,
            %(maximum_temp_celsius)s,
            %(pressure)s,
            %(humidity)s,
            %(wind_speed)s,
            %(observed_at_local)s,
            %(observed_date)s,
            %(sunrise_local)s,
            %(sunset_local)s,
            %(created_at)s,
            %(snapshot_date)s
        )
        ON DUPLICATE KEY UPDATE
            city = VALUES(city),
            region = VALUES(region),
            country_code = VALUES(country_code),
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
    cursor.executemany(insert_sql, weather_records)
    conn.commit()
    cursor.close()
    conn.close()


def prepare_weather_email(**context) -> None:
    weather_records = context["ti"].xcom_pull(
        task_ids="transform_weather_data",
        key="weather_records",
    )
    if not weather_records:
        raise ValueError("No transformed weather records found for email preparation.")

    failed_provinces = context["ti"].xcom_pull(
        task_ids="extract_weather_data",
        key="failed_weather_provinces",
    ) or []
    df = pd.DataFrame(weather_records).sort_values(
        by="temperature_celsius", ascending=False
    )
    observed_at_slug = df["observed_at_local"].max().replace(" ", "_").replace(":", "-")
    csv_file_path = f"/tmp/thailand_weather_{observed_at_slug}.csv"
    df.to_csv(csv_file_path, index=False)

    summary = {
        "province_count": int(df["province"].nunique()),
        "failed_province_count": len(failed_provinces),
        "avg_temperature_celsius": round(float(df["temperature_celsius"].mean()), 2),
        "avg_humidity": round(float(df["humidity"].mean()), 2),
        "hottest_province": df.loc[df["temperature_celsius"].idxmax(), "province"],
        "coolest_province": df.loc[df["temperature_celsius"].idxmin(), "province"],
        "observed_at_local": df["observed_at_local"].max(),
    }
    preview_df = df[["province", "temperature_celsius", "humidity", "weather_description"]].head(10)

    context["ti"].xcom_push(key="report_file", value=csv_file_path)
    context["ti"].xcom_push(key="html_report", value=_build_weather_html(summary, preview_df))
    context["ti"].xcom_push(key="report_subject_date", value=summary["observed_at_local"])


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
    tags=["weather", "thailand", "api", "mysql", "notification"],
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
        subject="Thailand Weather Pipeline Success {{ ti.xcom_pull(task_ids='prepare_weather_email', key='report_subject_date') }}",
        html_content="{{ ti.xcom_pull(task_ids='prepare_weather_email', key='html_report') }}",
        files=["{{ ti.xcom_pull(task_ids='prepare_weather_email', key='report_file') }}"],
    )

    extract_task >> transform_task >> load_task >> prepare_email_task >> send_success_email_task
