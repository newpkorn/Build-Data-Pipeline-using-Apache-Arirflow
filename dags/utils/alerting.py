import requests
import logging
from airflow.models import Variable

def slack_alert(context):

    webhook = Variable.get("slack_webhook")

    dag = context['dag'].dag_id
    task = context['task_instance'].task_id

    message = f"""
🚨 Airflow Failure

DAG: {dag}
Task: {task}
Execution: {context['execution_date']}
"""

    requests.post(
        webhook,
        json={"text": message}
    )

def _is_terminal_task_failure(context) -> bool:
    """Alert only when the task is truly failed, not when it is being retried."""
    task_instance = context.get("task_instance")
    return getattr(task_instance, "state", None) == "failed"

def discord_alert(context):
    if not _is_terminal_task_failure(context):
        return

    try:
        webhook = Variable.get("discord_webhook", default_var=None)
        if not webhook:
            logging.warning("discord_webhook not configured in Airflow variables")
            return

        dag = context['dag'].dag_id
        task = context['task_instance'].task_id

        message = f"""
                    🚨 **Airflow Failure**

                    **DAG:** {dag}
                    **Task:** {task}
                    **Execution:** {context['execution_date']}
                """

        response = requests.post(
            webhook,
            json={"content": message},
            timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Discord alert: {str(e)}")


def discord_success_alert(context):
    try:
        webhook = Variable.get("discord_webhook", default_var=None)
        if not webhook:
            logging.warning("discord_webhook not configured in Airflow variables")
            return

        dag = context["dag"].dag_id
        dag_run = context.get("dag_run")
        run_id = dag_run.run_id if dag_run else "unknown"
        execution_date = context.get("logical_date") or context.get("execution_date")

        message = f"""
                    ✅ **Airflow Success**

                    **DAG:** {dag}
                    **Run ID:** {run_id}
                    **Execution:** {execution_date}
                """

        response = requests.post(
            webhook,
            json={"content": message},
            timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to send Discord success alert: {str(e)}")
