# Historical E-Commerce Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-4.2%20%2F%203.5-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-2.8+-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Snappy Parquet](https://img.shields.io/badge/Parquet-Snappy_Partitioned-green?style=for-the-badge)](https://parquet.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32+-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Pytest](https://img.shields.io/badge/Pytest-11_Passed-brightgreen?style=for-the-badge&logo=pytest&logoColor=white)](https://docs.pytest.org/)

An enterprise-grade, end-to-end historical data engineering pipeline built using **verified real-world retail transactions** from the **UCI Machine Learning Repository**. The pipeline ingests 541,909 raw historical records, performs distributed PySpark cleaning and schema enforcement, partitions curated records into Snappy Parquet, and batch loads dimension and fact tables into a MySQL 8.0 Data Warehouse orchestrated by Apache Airflow.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Business Problem](#business-problem)
3. [Verified Historical Data Source](#verified-historical-data-source)
4. [Source Schema vs. Derived Fields](#source-schema-vs-derived-fields)
5. [End-to-End Architecture](#end-to-end-architecture)
6. [Technology Stack](#technology-stack)
7. [Installation & Setup](#installation--setup)
8. [Extraction & Raw Preservation](#extraction--raw-preservation)
9. [PySpark Distributed Transformations](#pyspark-distributed-transformations)
10. [Partitioned Snappy Parquet Storage](#partitioned-snappy-parquet-storage)
11. [MySQL 8.0 Data Warehouse Modeling](#mysql-80-data-warehouse-modeling)
12. [Apache Airflow DAG Orchestration](#apache-airflow-dag-orchestration)
13. [Interactive Streamlit Analytics Dashboard](#interactive-streamlit-analytics-dashboard)
14. [Data Quality & Test Suite](#data-quality--test-suite)
15. [Actual Measured Execution Results](#actual-measured-execution-results)
16. [Limitations & Scalability Strategies](#limitations--scalability-strategies)
17. [Senior Data Engineer Resume Bullets](#senior-data-engineer-resume-bullets)

---

## Project Overview

Most portfolio data engineering projects rely on toy synthetic data generators, randomized numbers, and mock local microservices that fail to reflect the messiness of production systems. 

This project rebuilds the data engineering pipeline around **authentic historical retail transactions** (541,909 rows) from an actual UK-based non-store online retailer operating between December 2010 and December 2011. It confronts real-world data engineering challenges:
- **Missing customer accounts**: ~25% of purchases are anonymous guest checkouts requiring nullable foreign key architectures rather than fabricated customer IDs.
- **Cancellations & Returns**: Transactions starting with `'C'` and negative quantities that must be flagged for accounting integrity rather than blindly dropped.
- **Deduplication & Lineage**: Removing exact transaction duplicates, computing cryptographic SHA-256 hashes, and storing audit metadata.
- **Dual Analytical Storage**: Snappy Parquet partitioned by `transaction_year` and `transaction_month` for big-data lakehouse queries, alongside relational dimensional tables in MySQL 8.0 for SQL BI reporting.

---

## Business Problem

Online retail enterprises require resilient data pipelines to analyze revenue, churn, return rates, and wholesale customer lifetime value. Typical operational bottlenecks include:
1. **Unreliable Inbound Feeds**: Raw data files frequently contain duplicate rows, missing customer identifiers, cancellations, and inconsistent timestamp formats.
2. **Double-Counting Revenue**: Ingesting return records with negative quantities as ordinary sales distorts gross revenue and inventory counts.
3. **Data Lake Sprawl**: Unpartitioned, uncompressed CSV storage leads to slow table scans and high storage costs.
4. **Non-Idempotent Pipelines**: Failed ETL jobs that re-execute without unique constraints lead to duplicate records and distorted financial metrics.

---

## Verified Historical Data Source

| Metadata Field | Verified Value |
| :--- | :--- |
| **Dataset Name** | Online Retail Dataset |
| **Repository** | UCI Machine Learning Repository |
| **Citation** | Daqing Chen, Sai Liang Sain, and Kun Guo (2012) |
| **Source URL** | [`https://archive.ics.uci.edu/dataset/352/online+retail`](https://archive.ics.uci.edu/dataset/352/online+retail) |
| **Direct Archive URL**| [`https://archive.ics.uci.edu/static/public/352/online+retail.zip`](https://archive.ics.uci.edu/static/public/352/online+retail.zip) |
| **Historical Period** | **December 1, 2010 – December 9, 2011** (1 year, 1 week) |
| **Actual Record Count**| **541,909 raw records** |
| **Raw Archive Size** | **23.7 MB** (compressed ZIP / XLSX) |
| **Raw CSV Size** | **45.81 MB** (48,039,726 bytes) |
| **License** | Creative Commons Attribution 4.0 International (**CC BY 4.0**) |
| **Business Context** | UK-based registered online retailer selling unique all-occasion gift-ware to international consumers and wholesalers. |

---

## Source Schema vs. Derived Fields

### Raw Source Schema (8 Columns)

| Column Name | Raw Type | Target Type | Description & Real-World Characteristic |
| :--- | :--- | :--- | :--- |
| `InvoiceNo` | String | `VARCHAR(20)` | 6-digit transaction ID. Prefixed with `'C'` for cancellations. |
| `StockCode` | String | `VARCHAR(30)` | 5-digit alphanumeric item identifier. |
| `Description` | String | `VARCHAR(255)` | Product description. Contains 1,454 nulls (0.27%) handled as `'UNKNOWN DESCRIPTION'`. |
| `Quantity` | Integer | `INT` | Units purchased per transaction. Negative values represent customer returns. |
| `InvoiceDate` | String | `DATETIME` | Timestamp (`yyyy-MM-dd HH:mm:ss` / `M/d/yyyy H:mm`). |
| `UnitPrice` | Decimal | `DECIMAL(10,2)`| Unit price in sterling (£). |
| `CustomerID` | Float/String | `INT NULL` | 5-digit customer ID. **135,080 rows (24.93%) are NULL** (guest checkouts). |
| `Country` | String | `VARCHAR(100)` | Customer country of residence (38 distinct countries). |

### Explicitly Derived Fields (Zero Fabricated Data)

1. **`is_cancellation`** (`TINYINT(1)`): Derived as `1` if `invoice_no` starts with `'C'` or `quantity < 0`, else `0`.
2. **`gross_amount`** (`DECIMAL(12,2)`): Calculated strictly as `quantity * unit_price`.
3. **`transaction_year`** (`SMALLINT`): Extracted `year(invoice_date)` (2010 or 2011) for partition structuring.
4. **`transaction_month`** (`TINYINT`): Extracted `month(invoice_date)` (1–12) for partition structuring.
5. **`ingestion_timestamp`** (`TIMESTAMP`): UTC audit timestamp recorded at ETL execution.

> [!NOTE]
> **No Fake Data Guarantee**: No artificial discounts, payment methods, or synthetic retail store IDs have been fabricated. Missing customer IDs are preserved as true relational `NULL` values.

---

## End-to-End Architecture

```mermaid
graph TD
    subgraph Raw Ingestion & Preservation Layer
        UCI["Verified Source: UCI ML Repository<br/>(online+retail.zip, 541,909 records)"] --> EXT["src/extract/historical_extractor.py<br/>(Resilient HTTP stream, SHA-256 verification)"]
        EXT --> RAW_CSV["data/raw/online_retail_raw.csv<br/>+ data/raw/source_metadata.json"]
        RAW_CSV --> VAL_RAW["src/validation/raw_validator.py<br/>(File size, schema headers, null distribution)"]
    end

    subgraph PySpark Transformation Engine
        VAL_RAW --> SPARK["src/spark/historical_sales_transformations.py<br/>- Explicit StructType Schema<br/>- Exact Deduplication (dropDuplicates)<br/>- Trim text & parse multi-format timestamps<br/>- Flag cancellations (is_cancellation)<br/>- Calculate gross_amount = Quantity * UnitPrice<br/>- Extract transaction_year & transaction_month"]
    end

    subgraph Curated Data Lake Layer
        SPARK --> PARQUET["Snappy Parquet Data Lake<br/>data/processed/historical_sales/<br/>transaction_year=YYYY/transaction_month=MM/"]
        PARQUET --> VAL_PARQ["Parquet Readback Validation<br/>(Row count reconciliation & partition checks)"]
    end

    subgraph Data Warehouse Layer
        VAL_PARQ --> LOADER["src/load/historical_mysql_loader.py<br/>(Batched, Idempotent UPSERT)"]
        LOADER --> FACT["fact_sales<br/>(invoice_no, stock_code, quantity, unit_price,<br/>gross_amount, invoice_date, customer_id, country)"]
        LOADER --> DIM_P["dim_products<br/>(stock_code, description, latest_unit_price, units_sold)"]
        LOADER --> DIM_C["dim_customers<br/>(customer_id, country, total_spend, order_count)"]
        LOADER --> LOGS["etl_pipeline_logs<br/>(Run telemetry, status, row metrics)"]
    end

    subgraph Airflow Orchestrator
        DAG["dags/historical_ecommerce_pipeline.py<br/>(10 Orchestrated Airflow Tasks)"] -.-> EXT
        DAG -.-> VAL_RAW
        DAG -.-> SPARK
        DAG -.-> VAL_PARQ
        DAG -.-> LOADER
        DAG -.-> DQ["tests/test_data_quality.py<br/>(11 Automated Pytest Assertions)"]
    end
```

---

## Technology Stack

- **Data Processing**: Python 3.11, Apache Spark (PySpark 4.2.0 / 3.5.0), Pandas 2.3.3, PyArrow 25.0.1
- **Database & Warehouse**: MySQL 8.0+ (InnoDB, UTF-8 MB4)
- **Orchestration**: Apache Airflow 2.8+ / 3.0
- **Analytics & BI Frontend**: Streamlit 1.32+, Plotly 5.20+
- **Data Lake Storage**: Apache Parquet with Snappy compression (Hive-style partitioning)
- **HTTP / Extraction**: Python Requests with urllib3 automatic retries
- **Testing & Validation**: Pytest 9.1.1, Pytest-Mock
- **Containerization**: Docker Compose

---

## Installation & Setup

### Prerequisites
- Python 3.11+
- Java 17+ (required for PySpark local execution)
- MySQL 8.0+ running locally on port 3306 (or via Docker)

### 1. Clone & Configure Virtual Environment
```bash
git clone https://github.com/abhaykumar/Ecommerce-Data-Pipeline.git
cd Ecommerce-Data-Pipeline

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and verify database settings:
```bash
cp .env.example .env
```
Default `.env` configuration:
```ini
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=ecommerce_dw
MYSQL_USER=root
MYSQL_PASSWORD=
HISTORICAL_SOURCE_URL=https://archive.ics.uci.edu/static/public/352/online+retail.zip
```

### 3. Initialize MySQL 8.0 Warehouse Schema
```bash
mysql -u root < sql/schema.sql
```

---

## Extraction & Raw Preservation

The module [`src/extract/historical_extractor.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/src/extract/historical_extractor.py) downloads and preserves the raw historical dataset:
```bash
.venv/bin/python src/extract/historical_extractor.py
```
Key capabilities:
- **Resilient Streaming**: Downloads the 23.7 MB archive using chunked streams with exponential backoff retries.
- **Pristine Preservation**: Extracts `Online Retail.xlsx` and converts it into pure raw CSV at `data/raw/online_retail_raw.csv`.
- **Audit Lineage**: Generates `data/raw/source_metadata.json` recording source URL, UTC timestamp, byte sizes, and cryptographic SHA-256 hash (`c35975c40d0e1e10...`).

Raw validation can be triggered independently:
```bash
.venv/bin/python src/validation/raw_validator.py
```

---

## PySpark Distributed Transformations

The transformation engine in [`src/spark/historical_sales_transformations.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/src/spark/historical_sales_transformations.py) processes the raw data using Spark DataFrames without full memory collects:
```bash
.venv/bin/python src/spark/historical_sales_transformations.py
```
Transformations performed:
1. **Explicit Schema Enforcement**: Uses `StructType` to prevent costly schema inference.
2. **Deduplication**: Identifies and drops 5,429 exact duplicate records on `["invoice_no", "stock_code", "invoice_date", "quantity"]`.
3. **Timestamp Standardization**: Parses multiple datetime formats (`yyyy-MM-dd HH:mm:ss`, `M/d/yyyy H:mm`, `dd/MM/yyyy HH:mm`) into uniform UTC `TimestampType`.
4. **Cancellation Logic**: Flags returns with `is_cancellation = 1`.
5. **Gross Amount**: Computes `quantity * unit_price` as `DecimalType(12, 2)`.
6. **Partition Extraction**: Extracts `transaction_year` and `transaction_month`.

---

## Partitioned Snappy Parquet Storage

Curated transactions are written to `data/processed/historical_sales/`:
- **Format**: Apache Parquet
- **Compression**: Snappy
- **Partition Scheme**: `transaction_year=YYYY/transaction_month=MM/`
- **Small File Control**: `df.repartition("transaction_year", "transaction_month")` ensures balanced, query-optimized partition files.

```
data/processed/historical_sales/
├── transaction_year=2010/
│   └── transaction_month=12/
│       └── part-*.parquet
└── transaction_year=2011/
    ├── transaction_month=1/
    ├── transaction_month=2/
    ├── ...
    └── transaction_month=12/
```
**Storage Optimization**: Raw CSV (45.81 MB) compressed down to **5.0 MB** in Parquet (a **9.2x compression ratio**).

---

## MySQL 8.0 Data Warehouse Modeling

The relational warehouse layer is managed by [`src/load/historical_mysql_loader.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/src/load/historical_mysql_loader.py):
```bash
.venv/bin/python src/load/historical_mysql_loader.py
```

### Table Definitions
1. **`fact_sales`** (536,480 rows): Granular transactional line items. Idempotent loading via composite unique key `(invoice_no, stock_code, invoice_date, quantity)` using `INSERT IGNORE` in streaming 10,000-record batches.
2. **`dim_products`** (3,958 rows): Product dimension capturing distinct stock codes, latest unit prices, first/last sold dates, and cumulative units sold. Loaded via `ON DUPLICATE KEY UPDATE`.
3. **`dim_customers`** (4,372 rows): Registered customer dimension with order counts, lifetime spend, and geographical residence.
4. **`etl_pipeline_logs`**: Audit telemetry capturing run IDs, timestamps, records processed, and status.

---

## Apache Airflow DAG Orchestration

The workflow is orchestrated in [`dags/historical_ecommerce_pipeline.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/dags/historical_ecommerce_pipeline.py) under DAG ID `historical_ecommerce_pipeline`:

```mermaid
graph LR
    T1[1. validate_configuration] --> T2[2. extract_historical_data]
    T2 --> T3[3. validate_raw_data]
    T3 --> T4[4. inspect_source_schema]
    T4 --> T5[5. process_data_with_pyspark]
    T5 --> T6[6. write_parquet_output]
    T6 --> T7[7. validate_parquet_output]
    T7 --> T8[8. load_mysql_tables]
    T8 --> T9[9. run_data_quality_checks]
    T9 --> T10[10. log_pipeline_status]
```

### Task Descriptions
1. `validate_configuration`: Asserts directory paths, environment variables, and MySQL engine availability.
2. `extract_historical_data`: Downloads and unpacks the raw archive into pristine CSV format.
3. `validate_raw_data`: Validates raw file size (>20MB), headers, row count, and null distributions.
4. `inspect_source_schema`: Logs source data dictionary and attributes.
5. `process_data_with_pyspark`: Cleans strings, parses timestamps, removes duplicates, and derives metrics.
6. `write_parquet_output`: Writes partitioned Snappy Parquet files.
7. `validate_parquet_output`: Reads back Parquet data and verifies record counts and partition structure.
8. `load_mysql_tables`: Idempotently loads `dim_products`, `dim_customers`, and `fact_sales` into MySQL 8.0.
9. `run_data_quality_checks`: Validates non-empty warehouse tables, non-negative unit prices, and referential integrity.
10. `log_pipeline_status`: Writes execution metrics to `etl_pipeline_logs`.

---

## Interactive E-Commerce Sales & Revenue Intelligence Dashboard

The application layer includes an executive-grade retail analytics dashboard implemented in [`app.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/app.py) using **Streamlit** and **Plotly**, styled with an engineering-first **Zinc minimal design system**.

### Accessing the Dashboard

1. **Public Live Link (Accessible on ANY laptop, phone, or computer worldwide)**:
   > 🌐 **[https://pete-newfoundland-podcast-philip.trycloudflare.com](https://pete-newfoundland-podcast-philip.trycloudflare.com)**

2. **Local Wi-Fi Network URL (For devices on the same Wi-Fi)**:
   > 💻 **`http://192.168.1.6:8501`**

3. **Local Machine**:
   > 🖥️ **`http://localhost:8501`**

### Launching / Exposing the Dashboard

```bash
# Activate virtual environment
source .venv/bin/activate

# 1. Start Streamlit server
streamlit run app.py --server.port 8501

# 2. To generate a public link to open on another laptop (in a new terminal):
cloudflared tunnel --url http://localhost:8501
```

### Sales Intelligence Modules

1. **📊 Executive Sales Overview**:
   - Primary Revenue & Volume KPIs: Net Realized Sales (£8.89M), Gross Sales Volume (£9.73M), Fulfillable Orders (22,060), Average Order Value (£440.92), Physical Inventory Units Shipped (5.18M), Order Cancellation/Refund Rate (3.9% / £833k refunded), Active Buyers (4,372 accounts), and Anonymous Guest Checkout share (~15.2% of sales).
   - Global Quick Filters: Filter by Sales Horizon (All, 2011, 2010) and Sales Region (Global, United Kingdom, International Exports).
   - Monthly Net Revenue Trajectory Area Chart and Dual-Axis Monthly Orders & AOV.
   - Retail Shopping Behavior: Sales by Day of Week (peak Thursdays & Tuesdays) and Hourly Order Volume (peak 10:00 - 15:00 UTC).

2. **🛍️ Products & Merchandise Intelligence**:
   - Best-Selling Products Leaderboard: Dynamic ranking by Gross Sales or Physical Units Sold, customizable depth (Top 5 to 25).
   - Item Price Tier Distribution: Revenue contribution across Budget (<£2), Standard (£2-£5), Premium (£5-£15), and Luxury (>£15).
   - Product Catalog Search: Sub-second lookup of any stock code or product keyword against the product dimension.

3. **👥 Customer Value & Segments**:
   - Customer Lifetime Value (CLV) Tiers: VIP (>£10k), High-Value (£2k-£10k), Mid-Tier (£500-£2k), Budget (<£500).
   - Pareto 80/20 Breakdown: Top VIP buyers (97 accounts) generating ~38% of registered revenue.
   - Top 10 VIP Accounts Leaderboard with lifetime spend, average basket size, and purchase recency.

4. **🌍 Regional & Export Sales**:
   - Domestic (United Kingdom ~84%) vs. International Cross-Border Exports (~16%).
   - Top 10 Export Destinations (Netherlands, Germany, France, Ireland, Spain, etc.).
   - Full Country Performance Matrix with order count, units shipped, gross sales, and average order value.

5. **🧾 Sales Orders Explorer**:
   - Real-time search and filter for transactional orders by Invoice Number, Customer ID, Description, Country, and Status (Completed vs. Returns).
   - Instant CSV export of filtered sales records.

6. **🌓 Theme Switcher**:
   - Instant 1-click toggle between Dark Mode (`#09090b` Zinc) and Light Mode (`#ffffff` Slate) with automatic Plotly chart theme synchronization.

---

## Data Quality & Test Suite

The test suite in [`tests/test_data_quality.py`](file:///Users/abhaykumar/Documents/Ecommerce-Data-Pipeline/tests/test_data_quality.py) executes 11 automated pytest assertions:
```bash
.venv/bin/pytest -v tests/test_data_quality.py
```

### Test Results (Actual Run)
```
tests/test_data_quality.py::TestRawDataIngestionQuality::test_raw_file_presence_and_metadata PASSED [  9%]
tests/test_data_quality.py::TestRawDataIngestionQuality::test_raw_columns_and_non_empty PASSED [ 18%]
tests/test_data_quality.py::TestPySparkTransformations::test_transformation_derivation_logic PASSED [ 27%]
tests/test_data_quality.py::TestPySparkTransformations::test_deduplication PASSED [ 36%]
tests/test_data_quality.py::TestParquetStorageQuality::test_parquet_directory_and_partitions PASSED [ 45%]
tests/test_data_quality.py::TestParquetStorageQuality::test_parquet_record_counts PASSED [ 54%]
tests/test_data_quality.py::TestDataWarehouseIntegrity::test_fact_sales_row_count PASSED [ 63%]
tests/test_data_quality.py::TestDataWarehouseIntegrity::test_dim_products_row_count_and_validity PASSED [ 72%]
tests/test_data_quality.py::TestDataWarehouseIntegrity::test_dim_customers_validity PASSED [ 81%]
tests/test_data_quality.py::TestDataWarehouseIntegrity::test_customer_referential_consistency PASSED [ 90%]
tests/test_data_quality.py::TestDataWarehouseIntegrity::test_zero_synthetic_data_columns PASSED [100%]

============================= 11 passed in 16.77s ==============================
```

---

## Actual Measured Execution Results

The following metrics represent real, measured benchmarks on the host machine:

| Metric Category | Measured Value |
| :--- | :--- |
| **Raw Source Records Ingested** | **541,909 rows** |
| **Duplicates Detected & Dropped** | **5,429 duplicate rows** |
| **Final Curated Records** | **536,480 rows** |
| **Unique Invoices** | **25,900 invoices** |
| **Unique Products (`dim_products`)** | **3,958 products** |
| **Registered Customers (`dim_customers`)** | **4,372 customers** |
| **Guest Checkouts (`CustomerID` is NULL)** | **134,931 records (25.15%)** |
| **Cancellations Flagged (`is_cancellation = 1`)**| **10,513 records** |
| **Historical Date Range** | **2010-12-01 13:56:00 to 2011-12-09 18:20:00** |
| **Total Gross Sales Value** | **£9,726,711.14 GBP** |
| **Raw CSV File Size** | **45.81 MB** |
| **Curated Snappy Parquet Size** | **5.0 MB** (**9.2x compression ratio**) |
| **PySpark Transformation Duration** | **~32 seconds** |
| **MySQL Warehouse Batch Loading Duration** | **~48 seconds** (536,480 fact rows) |
| **Pytest Full Suite Execution** | **16.77 seconds** |

---

## Limitations & Scalability Strategies

### Current Dataset Characteristics
- While 541,909 records provide authentic real-world complexity (cancellations, guest checkouts, international multi-currency pricing), half a million records run comfortably in a local single-node PySpark environment.
- On macOS (8GB RAM, Apple Silicon), the entire end-to-end pipeline executes in ~90 seconds.

### Horizontal Scaling to Multi-Million / Billion Rows
1. **Cluster PySpark Execution**: Transition `local[*]` master to a managed Dataproc or Amazon EMR cluster with autoscaling worker nodes.
2. **Cloud Object Storage**: Transition `data/processed/historical_sales/` to Google Cloud Storage (`gs://bucket/`) or AWS S3 (`s3://bucket/`).
3. **Columnar MPP Warehouse**: For datasets exceeding 100M+ rows, replace single-instance MySQL fact tables with Google BigQuery, Snowflake, or AWS Redshift.
4. **Incremental Micro-Batching**: Implement Change Data Capture (CDC) with Apache Kafka and Spark Structured Streaming for real-time order processing.

---

## Senior Data Engineer Resume Bullets

- **Architected and implemented** an end-to-end historical data engineering pipeline using PySpark, Parquet, MySQL 8.0, and Apache Airflow, processing **541,909 real-world retail transactions** from the UCI Machine Learning Repository.
- **Engineered distributed PySpark transformation workflows** enforcing explicit `StructType` schemas, automated deduplication, multi-format timestamp parsing, and financial metric derivations without in-memory driver bottlenecks.
- **Designed a partitioned Lakehouse storage layer** utilizing Snappy-compressed Parquet partitioned by `transaction_year` and `transaction_month`, achieving a **9.2x compression ratio** (45.8 MB down to 5.0 MB).
- **Modeled a high-performance relational warehouse** in MySQL 8.0 featuring `fact_sales` (536,480 rows), `dim_products` (3,958 rows), and `dim_customers` (4,372 rows) with idempotent batch upserts and sub-minute load times.
- **Implemented a comprehensive test suite** with 11 automated pytest checks verifying schema enforcement, null distribution audits (24.9% guest orders), referential consistency, and zero synthetic artifact leakage.
