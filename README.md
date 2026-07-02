# ETL Pipeline — Amman Digital Market

## Overview

This project implements a production-style ETL (Extract, Transform, Load) pipeline for the **Amman Digital Market** database.

The pipeline is designed to:
- Extract data from PostgreSQL
- Transform it into analytical datasets using Pandas
- Validate data quality
- Load results into both a database table and CSV files

The system evolves across three tiers:
- **Tier 1:** Data transformation and quality reporting
- **Tier 2:** Incremental ETL with metadata tracking
- **Tier 3:** Config-driven ETL framework with structured logging

---

## Architecture

The pipeline follows a modular architecture:

Extract → Transform → Validate → Load → Log Metadata

Additionally:
- JSON configuration drives pipeline behavior (Tier 3)
- Logging replaces print statements for production-style observability
- Metadata tracking enables incremental processing

---

## Setup

### 1. Start PostgreSQL container

```bash
docker run -d --name postgres-m3-int \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=amman_market \
  -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
  postgres:15-alpine
```

### 2. Load schema and seed data

```bash
docker exec -i postgres-m3-int psql -U postgres -d amman_market < schema.sql
docker exec -i postgres-m3-int psql -U postgres -d amman_market < seed_data.sql
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## How to Run

Run the pipeline using:

```bash
python "challenges etl_pipeline.py"
```

---

## Outputs

### 1. PostgreSQL Tables
- `customer_analytics`
- `product_analytics` (second pipeline via config)

### 2. CSV Files
- `output/customer_analytics.csv`
- `output/product_analytics.csv`

### 3. Quality Reports
- JSON reports generated per pipeline run  
  (e.g. `output/customer_quality.json`)

---

## Data Transformations

The pipeline performs:

### Joins across:
- customers  
- orders  
- order_items  
- products  

### Calculations:
- `line_total = quantity * unit_price`

### Filters:
- Exclude cancelled orders  
- Exclude suspicious quantities (>100)

### Aggregations:
Customer-level metrics:
- total_orders  
- total_revenue  
- avg_order_value  
- top_category  

---

## Data Quality Checks (Tier 1)

The pipeline validates:

- No null values in critical fields  
- No duplicate primary keys  
- Positive revenue values  
- Positive order counts  

### Outlier Detection
Customers with revenue > 3 standard deviations above the mean are flagged.

---

## Quality Report

A JSON report includes:

- Validation results (PASS/FAIL)  
- Failed checks (if any)  
- Outliers detected  
- Timestamp of execution  

---

## Incremental ETL (Tier 2)

The pipeline supports incremental loading:

- Only processes orders newer than the last successful run  
- Uses the `etl_metadata` table for tracking  

### Metadata Table

| Column           | Description                    |
|------------------|--------------------------------|
| run_id           | Unique run identifier          |
| start_time       | ETL start timestamp            |
| end_time         | ETL end timestamp              |
| rows_processed   | Number of processed rows       |
| status           | success / failed               |

---

## Execution Summary

### Full Load Run
- Rows after transform: 85  
- Rows loaded: 85  

### Incremental Run
- Rows after transform: 0  
- Rows loaded: 0  
- Reason: No new orders since the last ETL run  

This confirms that incremental logic correctly skips previously processed data.

---

## Full vs Incremental Tradeoffs

### Full Load
- Processes entire dataset  
- More reliable  
- Slower at scale  

### Incremental Load
- Processes only new data  
- Faster and efficient  
- Requires metadata tracking  

---

## ETL Framework (Tier 3)

The pipeline is config-driven, meaning:

- No code changes are needed to build a new pipeline  
- Behavior is controlled via JSON config files  

### Config Includes:
- Source tables  
- Join logic  
- Filters  
- Calculations  
- Aggregations  
- Output targets  

---

## Multiple Pipelines

Two pipelines are implemented:

### 1. Customer Analytics
- Aggregates data per customer  

### 2. Product Analytics
- Aggregates data per product/category  

Both pipelines run using the same ETL engine with different configs.

---

## Logging

The pipeline uses Python’s logging module.

### Example log:

```
2026-04-03 18:29:26 | INFO | Transforming data
2026-04-03 18:29:26 | INFO | Rows after transform: 85
```

### Benefits:
- Timestamped logs  
- Clear pipeline stages  
- Production-ready monitoring  

---

## Design Goal

The system is designed to be:

- Reusable  
- Scalable  
- Configurable  
- Production-oriented  

Adding a new pipeline requires:

**Only a new JSON config file — no changes to Python code**

---

## License

This project is for educational use only.

You may use it for learning, practice, and portfolio demonstration.