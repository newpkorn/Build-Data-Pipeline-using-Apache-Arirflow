# 🚀 Production-Ready CSV to MySQL Data Pipeline

![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-red)
![MySQL](https://img.shields.io/badge/MySQL-Database-orange)
![Status](https://img.shields.io/badge/Status-Portfolio--Ready-success)

------------------------------------------------------------------------

## 📌 Overview

A production-style Data Engineering project built with Apache Airflow,
Docker, and MySQL.

This pipeline:

-   Ingests CSV data
-   Loads data into MySQL
-   Uses PostgreSQL as Airflow metadata database
-   Is fully containerized
-   Supports environment-based configuration

------------------------------------------------------------------------

## 🎨 Architecture Diagram

![Architecture](architecture.png)

------------------------------------------------------------------------

## 🧩 ER Diagram

![ER Diagram](er_diagram.png)

------------------------------------------------------------------------

## 📸 Airflow UI

![Airflow UI](airflow_ui.png)

------------------------------------------------------------------------

## ⚙️ How It Works

1.  Airflow Scheduler triggers DAG.
2.  DAG reads CSV file.
3.  Data is loaded into MySQL table.
4.  Logs and execution history are stored.

------------------------------------------------------------------------

## 🐳 Run the Project

``` bash
docker compose up
```

Open:

http://localhost:8080

Trigger DAG: `csv_to_mysql`

------------------------------------------------------------------------

## 💼 Portfolio Highlights

-   Containerized Infrastructure
-   Orchestrated Workflow
-   Database Integration
-   Clean Project Structure
-   Production Mindset Configuration
-   Observability via Logs

------------------------------------------------------------------------

## 🚀 Future Improvements

-   Incremental data loading
-   Data validation layer
-   CI/CD integration
-   Cloud deployment (AWS / GCP)
-   Custom Airflow Docker image

------------------------------------------------------------------------

## 📜 License

For educational and portfolio use.
