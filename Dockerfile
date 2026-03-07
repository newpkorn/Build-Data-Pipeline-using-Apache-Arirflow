ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.9
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt