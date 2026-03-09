ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.9
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         default-libmysqlclient-dev \
         pkg-config \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt