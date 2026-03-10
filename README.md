# Data Engineering Pipeline Platform with Apache Airflow

A production-style data engineering platform built with Apache Airflow and Docker.

This project demonstrates how to design, orchestrate, monitor, and test scalable ETL pipelines using modern data engineering practices including CI/CD, data quality validation, monitoring dashboards, and automated deployments.

---

# System Architecture

The platform processes data from multiple sources including CSV files and external APIs.
Apache Airflow orchestrates ETL pipelines which transform and load data into MySQL while providing monitoring and alerting capabilities.

![Architecture](docs/architecture.png)

## Architecture Layers

### 1. Data Sources

- CSV datasets
- External APIs
- Financial exchange rate APIs

### 2. Ingestion Layer

Located in:

```
dags/ingestion/
```

Responsible for loading raw datasets into the system.

Examples:

- CSV ingestion
- API data ingestion

### 3. Processing Layer

Located in:

```
dags/pipelines/
```

Contains business pipelines such as:

- Exchange rate analytics
- COVID-19 dataset processing
- Customer review sentiment analysis
- E-commerce analytics pipeline
- FX anomaly detection

### 4. Storage Layer

- MySQL → analytical datasets
- PostgreSQL → Airflow metadata

### 5. Observability Layer

- Grafana dashboards
- Discord alert notifications
- Airflow monitoring UI

---

# Data Model

Entity relationships are defined in the ER diagram.

![ER Diagram](docs/er_diagram.png)

---

# Airflow DAG Architecture

Airflow DAGs are organized into multiple logical groups.

```
dags/
├── ingestion
├── pipelines
└── utils
```

### Ingestion DAGs

```
csv_to_mysql.py
etl_api_pipeline.py
```

Purpose:

- Load raw datasets
- Normalize schema
- Store raw data

### Pipeline DAGs

Examples:

- exchange_rate_pipeline
- ecommerce_full_pipeline
- customer_review_sentiment_pipeline
- covid19_data_pipeline
- bot_exchange_rate_pipeline

Each pipeline follows the ETL structure:

```
Extract
   ↓
Transform
   ↓
Load
   ↓
Reporting / Alerts
```

### Utility Modules

Reusable components for pipelines.

Located in:

```
dags/utils/
```

Examples include:

- API clients
- database manager
- data quality validation
- anomaly detection
- schema management
- reporting utilities

---

# Project Structure

```
├── dags
│   ├── ingestion
│   ├── pipelines
│   ├── templates
│   └── utils
│
├── data
│   ├── people.csv
│   └── raw_customer_reviews.csv
│
├── docker
│   ├── airflow
│   └── mysql
│
├── grafana
│   ├── dashboards
│   └── queries
│
├── scripts
│   └── init_airflow.sh
│
├── tests
│   ├── test_data_quality.py
│   └── test_fx_pipeline.py
│
├── .github/workflows
│   └── airflow-ci.yml
│
├── docker-compose.yaml
├── requirements.txt
├── Makefile
├── README.md
```

---

# Environment Setup

Copy the environment template.

```
cp .env.example .env
```

Edit the environment variables if needed.

Example variables include:

```
POSTGRES_USER
MYSQL_USER
BOT_API_KEY
DISCORD_WEBHOOK
```

---

# Running the Platform

Start the platform using Docker Compose.

```
make up
```

or

```
docker compose up -d --build
```

Stop services

```
make down
```

View logs

```
make logs
```

---

# Access Services

Airflow Web UI

```
http://localhost:8080
```

Default credentials:

```
username: admin
password: admin
```

Grafana Dashboard

```
http://localhost:3000
```

---

# Airflow Initialization

Airflow initialization tasks are automated using:

```
scripts/init_airflow.sh
```

The script automatically:

- Runs database migrations
- Creates the admin user
- Configures database connections
- Registers Airflow variables

---

# Monitoring & Observability

Pipeline metrics and analytics are visualized using Grafana dashboards.

Located in:

```
grafana/dashboards/
```

Available dashboards include:

- FX Intelligence Dashboard
- FinTech FX Intelligence Platform
- Global Currency Exchange Intelligence
- Hedge Fund FX Monitoring
- Ultimate FX Portfolio Dashboard

Example Airflow UI:

![Airflow UI](docs/airflow_ui.png)

---

# Data Quality Validation

The platform includes built-in data quality validation.

Located in:

```
dags/utils/data_quality.py
```

Validation checks include:

- Null detection
- Duplicate detection
- Schema validation
- FX anomaly detection

Example usage:

```
from utils.data_quality import validate_dataset

validate_dataset(dataframe)
```

---

# Testing

Automated tests are implemented using pytest.

Test directory:

```
tests/
```

Run tests locally:

```
make test
```

or

```
pytest tests/
```

Test coverage includes:

- data quality checks
- FX analytics pipelines
- transformation logic

---

# CI/CD Pipeline

Continuous integration and deployment is handled via GitHub Actions.

Workflow file:

```
.github/workflows/airflow-ci.yml
```

CI pipeline steps:

1. Install dependencies
2. Run automated tests
3. Deploy to GCP VM via SSH
4. Rebuild Docker containers

Deployment command executed on the server:

```
docker compose down
docker compose up -d --build
```

---

# Example ETL Workflow

The following diagram illustrates the end-to-end ETL workflow orchestrated by Apache Airflow.

![ETL Pipeline](docs/dag_pipeline.png)

Typical pipeline execution:

```
Extract API / CSV Data
      ↓
Transform Data
      ↓
Validate Data Quality
      ↓
Load into MySQL
      ↓
Generate Reports
      ↓
Send Alerts
```

---

# Technologies Used

Core stack:

- Apache Airflow
- Docker
- MySQL
- PostgreSQL
- Grafana

Python ecosystem:

- Pandas
- Requests
- Pytest

Infrastructure:

- Docker Compose
- GitHub Actions
- GCP Virtual Machine

---

# Future Improvements

Potential enhancements:

- Kubernetes deployment
- dbt integration
- data warehouse layer
- advanced anomaly detection
- Slack / PagerDuty alerts

---

# License

MIT License
