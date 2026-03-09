#!/bin/bash
set -e

echo "Running DB migration..."
airflow db migrate

echo "Creating admin user..."

airflow users create \
  --username ${AIRFLOW_ADMIN_USERNAME} \
  --password ${AIRFLOW_ADMIN_PASSWORD} \
  --firstname ${AIRFLOW_ADMIN_FIRSTNAME} \
  --lastname ${AIRFLOW_ADMIN_LASTNAME} \
  --role Admin \
  --email ${AIRFLOW_ADMIN_EMAIL} \
|| true

echo "Setting MySQL connection..."

airflow connections delete mysql_default || true

airflow connections add mysql_default \
  --conn-type mysql \
  --conn-host mysql \
  --conn-port 3306 \
  --conn-login ${MYSQL_USER} \
  --conn-password ${MYSQL_PASSWORD} \
  --conn-schema ${MYSQL_DB} \
|| true

echo "Airflow init finished"