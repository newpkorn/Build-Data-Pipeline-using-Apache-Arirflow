from pathlib import Path
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.models import DagBag


ROOT_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = ROOT_DIR / "dags"
PIPELINES_DIR = DAGS_DIR / "pipelines"

for path in (DAGS_DIR, PIPELINES_DIR):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

import weather_api_pipeline as weather


def _build_task_instance():
    ti = MagicMock()
    ti.xcom_pull = MagicMock()
    ti.xcom_push = MagicMock()
    return ti


def test_weather_dag_exists():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("weather_api_pipeline")
    assert dag is not None


def test_weather_dag_structure():
    dag_bag = DagBag()
    dag = dag_bag.get_dag("weather_api_pipeline")

    assert dag is not None
    assert dag.task_ids == [
        "extract_weather_data",
        "transform_weather_data",
        "load_weather_data",
        "prepare_weather_email",
        "send_weather_success_email",
    ]

    assert dag.get_task("extract_weather_data").downstream_task_ids == {"transform_weather_data"}
    assert dag.get_task("transform_weather_data").downstream_task_ids == {"load_weather_data"}
    assert dag.get_task("load_weather_data").downstream_task_ids == {"prepare_weather_email"}
    assert dag.get_task("prepare_weather_email").downstream_task_ids == {"send_weather_success_email"}


@patch("weather_api_pipeline._get_variable", return_value="")
def test_get_weather_provinces_returns_default_list(mock_get_variable):
    provinces = weather._get_weather_provinces()

    assert provinces == weather.THAILAND_PROVINCES
    mock_get_variable.assert_called_once_with("weather_cities", "WEATHER_CITIES", "")


@patch("weather_api_pipeline._get_variable", return_value="Bangkok, Chiang Mai , Phuket")
def test_get_weather_provinces_returns_configured_list(mock_get_variable):
    provinces = weather._get_weather_provinces()

    assert provinces == ["Bangkok", "Chiang Mai", "Phuket"]
    mock_get_variable.assert_called_once_with("weather_cities", "WEATHER_CITIES", "")


@patch("weather_api_pipeline._get_variable", return_value="")
def test_extract_weather_data_raises_when_api_key_missing(mock_get_variable):
    ti = _build_task_instance()

    with pytest.raises(
        ValueError,
        match="OpenWeather API key",
    ):
        weather.extract_weather_data(ti=ti)

    mock_get_variable.assert_any_call("openweather_api_key", "OPENWEATHER_API_KEY")
    mock_get_variable.assert_any_call("weather_cities", "WEATHER_CITIES", "")


@patch("weather_api_pipeline.requests.Session")
@patch("weather_api_pipeline._get_weather_provinces", return_value=["Bangkok", "Chiang Mai"])
@patch("weather_api_pipeline._get_variable", return_value="test-api-key")
def test_extract_weather_data_pushes_payloads_and_failed_provinces(
    mock_get_variable,
    mock_get_weather_provinces,
    mock_session_cls,
):
    bangkok_response = MagicMock()
    bangkok_response.json.return_value = {"name": "Bangkok", "dt": 1, "timezone": 0}
    bangkok_response.raise_for_status.return_value = None

    session = MagicMock()
    session.get.side_effect = [
        bangkok_response,
        weather.requests.RequestException("province lookup failed"),
    ]
    mock_session_cls.return_value = session

    ti = _build_task_instance()

    payloads = weather.extract_weather_data(ti=ti)

    assert len(payloads) == 1
    assert payloads[0]["_requested_province"] == "Bangkok"
    ti.xcom_push.assert_any_call(key="weather_provinces", value=["Bangkok", "Chiang Mai"])
    ti.xcom_push.assert_any_call(key="failed_weather_provinces", value=["Chiang Mai"])
    ti.xcom_push.assert_any_call(key="raw_weather_payloads", value=payloads)
    mock_get_variable.assert_called_once_with("openweather_api_key", "OPENWEATHER_API_KEY")
    mock_get_weather_provinces.assert_called_once()


def test_transform_weather_data_returns_normalized_records():
    ti = _build_task_instance()
    ti.xcom_pull.return_value = [
        {
            "_requested_province": "Bangkok",
            "name": "Bangkok",
            "timezone": 25200,
            "dt": 1_710_000_000,
            "sys": {"country": "TH", "sunrise": 1_709_980_000, "sunset": 1_710_020_000},
            "weather": [{"description": "clear sky"}],
            "main": {
                "temp": 33.12,
                "feels_like": 38.2,
                "temp_min": 31.0,
                "temp_max": 34.0,
                "pressure": 1008,
                "humidity": 70,
            },
            "wind": {"speed": 3.7},
        }
    ]

    records = weather.transform_weather_data(ti=ti)

    assert len(records) == 1
    assert records[0]["province"] == "Bangkok"
    assert records[0]["region"] == "Central"
    assert records[0]["weather_description"] == "clear sky"
    assert records[0]["temperature_celsius"] == 33.12
    ti.xcom_push.assert_called_once_with(key="weather_records", value=records)


@patch("weather_api_pipeline.MySqlHook")
def test_load_weather_data_inserts_records(mock_mysql_hook):
    weather_records = [
        {
            "province": "Bangkok",
            "region": "Central",
            "city": "Bangkok",
            "country_code": "TH",
            "weather_description": "clear sky",
            "temperature_celsius": 33.12,
            "feels_like_celsius": 38.2,
            "minimum_temp_celsius": 31.0,
            "maximum_temp_celsius": 34.0,
            "pressure": 1008,
            "humidity": 70,
            "wind_speed": 3.7,
            "observed_at_local": "2026-03-25 09:00:00",
            "sunrise_local": "2026-03-25 06:15:00",
            "sunset_local": "2026-03-25 18:20:00",
            "created_at": "2026-03-25 09:01:00",
        }
    ]
    ti = _build_task_instance()
    ti.xcom_pull.return_value = weather_records

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    mock_mysql_hook.return_value.get_conn.return_value = conn

    weather.load_weather_data(ti=ti)

    cursor.executemany.assert_called_once()
    conn.commit.assert_called_once()
    cursor.close.assert_called_once()
    conn.close.assert_called_once()


def test_prepare_weather_email_pushes_report_artifacts():
    weather_records = [
        {
            "province": "Bangkok",
            "region": "Central",
            "city": "Bangkok",
            "country_code": "TH",
            "weather_description": "clear sky",
            "temperature_celsius": 33.12,
            "feels_like_celsius": 38.2,
            "minimum_temp_celsius": 31.0,
            "maximum_temp_celsius": 34.0,
            "pressure": 1008,
            "humidity": 70,
            "wind_speed": 3.7,
            "observed_at_local": "2026-03-25 09:00:00",
            "sunrise_local": "2026-03-25 06:15:00",
            "sunset_local": "2026-03-25 18:20:00",
            "created_at": "2026-03-25 09:01:00",
        },
        {
            "province": "Chiang Mai",
            "region": "North",
            "city": "Chiang Mai",
            "country_code": "TH",
            "weather_description": "cloudy",
            "temperature_celsius": 28.55,
            "feels_like_celsius": 31.1,
            "minimum_temp_celsius": 26.0,
            "maximum_temp_celsius": 29.0,
            "pressure": 1006,
            "humidity": 78,
            "wind_speed": 2.4,
            "observed_at_local": "2026-03-25 09:00:00",
            "sunrise_local": "2026-03-25 06:20:00",
            "sunset_local": "2026-03-25 18:24:00",
            "created_at": "2026-03-25 09:01:00",
        },
    ]
    ti = _build_task_instance()
    ti.xcom_pull.side_effect = [weather_records, ["Phuket"]]

    weather.prepare_weather_email(ti=ti)

    pushed = {
        call.kwargs["key"]: call.kwargs["value"]
        for call in ti.xcom_push.call_args_list
    }

    assert pushed["report_file"].startswith("/tmp/thailand_weather_")
    assert pushed["report_file"].endswith(".csv")
    assert "Thailand Weather Pipeline Completed Successfully" in pushed["html_report"]
    assert pushed["report_subject_date"] == "2026-03-25 09:00:00"


def test_prepare_weather_email_raises_when_records_missing():
    ti = _build_task_instance()
    ti.xcom_pull.return_value = []

    with pytest.raises(ValueError, match="No transformed weather records found for email preparation."):
        weather.prepare_weather_email(ti=ti)
