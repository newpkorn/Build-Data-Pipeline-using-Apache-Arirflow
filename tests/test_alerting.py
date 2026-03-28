import sys
import types
from types import SimpleNamespace
from unittest.mock import patch


airflow_module = types.ModuleType("airflow")
airflow_models_module = types.ModuleType("airflow.models")


class DummyVariable:
    @staticmethod
    def get(name):
        raise NotImplementedError


airflow_models_module.Variable = DummyVariable
airflow_module.models = airflow_models_module
sys.modules.setdefault("airflow", airflow_module)
sys.modules.setdefault("airflow.models", airflow_models_module)

from utils.alerting import discord_alert


@patch("utils.alerting.requests.post")
@patch("utils.alerting.Variable.get", return_value="https://example.com/webhook")
def test_discord_alert_skips_retry_state(mock_variable_get, mock_post):
    context = {
        "dag": SimpleNamespace(dag_id="weather_api_pipeline"),
        "task_instance": SimpleNamespace(task_id="extract_weather_data", state="up_for_retry"),
        "execution_date": "2026-03-27T17:00:00+00:00",
    }

    discord_alert(context)

    mock_variable_get.assert_not_called()
    mock_post.assert_not_called()


@patch("utils.alerting.requests.post")
@patch("utils.alerting.Variable.get", return_value="https://example.com/webhook")
def test_discord_alert_posts_on_terminal_failure(mock_variable_get, mock_post):
    context = {
        "dag": SimpleNamespace(dag_id="weather_api_pipeline"),
        "task_instance": SimpleNamespace(task_id="extract_weather_data", state="failed"),
        "execution_date": "2026-03-27T17:00:00+00:00",
    }

    discord_alert(context)

    mock_variable_get.assert_called_once_with("discord_webhook")
    mock_post.assert_called_once()
