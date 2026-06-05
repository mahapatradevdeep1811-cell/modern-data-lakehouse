# 🏗️ Modern Data Lakehouse Pipeline

A production-grade, PySpark-based ETL framework for building a Modern Data Lakehouse with support for **Snowflake** and **BigQuery** — switchable via a single config toggle. Orchestrated with **Apache Airflow**.

---

## 📐 Architecture Overview

```
Raw Sources (CSV / JSON / Parquet / API)
        │
        ▼
  ┌─────────────┐
  │  Extractors  │  ← PySpark readers (structured + semi-structured)
  └──────┬──────┘
         │
         ▼
  ┌──────────────┐
  │ Transformers  │  ← Spark SQL + DataFrame API transformations
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │ Data Quality  │  ← Validation, reconciliation, anomaly checks
  └──────┬───────┘
         │
         ▼
  ┌──────────────┐
  │   Loaders    │  ← Snowflake or BigQuery (config-driven)
  └──────┬───────┘
         │
         ▼
  ┌──────────────────────┐
  │  Airflow Orchestration│  ← DAGs, sensors, retry logic
  └──────────────────────┘
```

---

## 🗂️ Project Structure

```
modern-data-lakehouse/
├── config/                    # Environment & warehouse configs
│   ├── base.yaml
│   ├── snowflake.yaml
│   └── bigquery.yaml
├── etl/
│   ├── extractors/            # Source readers (CSV, JSON, Parquet, API)
│   ├── transformers/          # Spark SQL & DataFrame transformations
│   └── loaders/               # Snowflake & BigQuery writers
├── dq/
│   ├── checks/                # Schema, null, range, uniqueness checks
│   └── reconciliation/        # Source-to-target row/sum reconciliation
├── orchestration/
│   ├── dags/                  # Airflow DAG definitions
│   └── plugins/               # Custom Airflow operators & hooks
├── utils/                     # Logging, Spark session factory, helpers
├── tests/
│   ├── unit/
│   └── integration/
├── notebooks/                 # Exploratory analysis
├── scripts/                   # Bootstrap & utility scripts
├── docs/                      # Architecture & runbook docs
├── .env.example
├── docker-compose.yml         # Airflow + Spark local dev stack
├── requirements.txt
├── setup.py
└── Makefile
```

---

## ⚙️ Configuration Toggle

Switch between Snowflake and BigQuery in `config/base.yaml`:

```yaml
warehouse:
  target: snowflake   # or "bigquery"
```

All loaders and connection pools respect this single flag.

---

## 🚀 Quick Start

### 1. Clone & install
```bash
git clone https://github.com/<your-username>/modern-data-lakehouse.git
cd modern-data-lakehouse
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Fill in your Snowflake / BigQuery credentials
```

### 3. Start local stack (Airflow + Spark)
```bash
make up
```

### 4. Run an ETL pipeline manually
```bash
python -m etl.pipeline --source data/sample_orders.csv --target orders_curated
```

### 5. Trigger via Airflow
Open [http://localhost:8080](http://localhost:8080), enable `lakehouse_main_dag`, and trigger a run.

---

## 🧪 Testing

```bash
make test          # All tests
make test-unit     # Unit only
make test-int      # Integration only
```

---

## 📦 Tech Stack

| Layer | Technology |
|-------|-----------|
| Processing | Apache Spark (PySpark) |
| SQL Engine | Spark SQL |
| Warehouse A | Snowflake (`snowflake-connector-python`) |
| Warehouse B | Google BigQuery (`google-cloud-bigquery`) |
| Orchestration | Apache Airflow 2.x |
| Data Quality | Custom DQ framework + Great Expectations (optional) |
| Containerization | Docker + Docker Compose |
| Testing | pytest + pyspark testing utilities |
| Config | PyYAML + python-dotenv |

---

## 📄 License

MIT
