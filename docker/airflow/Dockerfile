ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.9
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Add this section right before the "COPY requirements.txt /" line

# Switch to root user to install system packages
USER root

# Install dependencies for matplotlib and pyarrow
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  cmake \
  libfreetype6-dev \
  libpng-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt