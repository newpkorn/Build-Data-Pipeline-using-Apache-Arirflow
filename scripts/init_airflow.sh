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

echo "Setting Airflow variables..."

airflow variables set bot_api_key ${BOT_API_KEY} || true
airflow variables set discord_webhook ${DISCORD_WEBHOOK} || true
airflow variables set openweather_api_key ${OPENWEATHER_API_KEY} || true
airflow variables set weather_city ${WEATHER_CITY:-Portland} || true
airflow variables set weather_alert_email ${WEATHER_ALERT_EMAIL:-airflow@example.com} || true

echo "Airflow init finished"
