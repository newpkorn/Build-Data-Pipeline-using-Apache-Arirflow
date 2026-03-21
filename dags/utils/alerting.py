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

def discord_alert(context):

    webhook = Variable.get("discord_webhook")

    dag = context['dag'].dag_id
    task = context['task_instance'].task_id

    message = f"""
🚨 **Airflow Failure**

**DAG:** {dag}
**Task:** {task}
**Execution:** {context['execution_date']}
"""

    requests.post(
        webhook,
        json={"content": message}
    )


def discord_success_alert(context):

    webhook = Variable.get("discord_webhook")

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

    requests.post(
        webhook,
        json={"content": message}
    )
