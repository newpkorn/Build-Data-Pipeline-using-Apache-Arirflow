import requests
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