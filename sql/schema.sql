-- =====================================================================
-- HISTORICAL E-COMMERCE DATA WAREHOUSE SCHEMA (MySQL 8.0+)
-- Database: ecommerce_dw
-- Based on: Verified UCI Machine Learning Repository Online Retail Dataset
-- =====================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

-- ---------------------------------------------------------------------
-- 1. Product Dimension (dim_products)
-- Derived strictly from verified historical stock transactions
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    stock_code VARCHAR(30) NOT NULL,
    description VARCHAR(255) NULL,
    latest_unit_price DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    first_sold_at DATETIME NULL,
    last_sold_at DATETIME NULL,
    total_units_sold INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (stock_code),
    INDEX idx_dim_prod_desc (description)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- 2. Customer Dimension (dim_customers)
-- Derived strictly from registered historical customers (non-null CustomerIDs)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT NOT NULL,
    country VARCHAR(100) NOT NULL,
    total_orders INT NOT NULL DEFAULT 0,
    total_spend DECIMAL(14, 2) NOT NULL DEFAULT 0.00,
    first_order_date DATETIME NULL,
    last_order_date DATETIME NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id),
    INDEX idx_dim_cust_country (country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- 3. Historical Sales Fact Table (fact_sales)
-- Models granular historical sales and cancellations
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    invoice_no VARCHAR(20) NOT NULL,
    stock_code VARCHAR(30) NOT NULL,
    description VARCHAR(255) NULL,
    quantity INT NOT NULL,
    invoice_date DATETIME NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    customer_id INT NULL,
    country VARCHAR(100) NOT NULL,
    gross_amount DECIMAL(12, 2) NOT NULL,
    is_cancellation TINYINT(1) NOT NULL DEFAULT 0,
    transaction_year SMALLINT NOT NULL,
    transaction_month TINYINT NOT NULL,
    source_system VARCHAR(50) DEFAULT 'UCI_ONLINE_RETAIL',
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_transaction_line (invoice_no, stock_code, invoice_date, quantity),
    INDEX idx_fact_invoice (invoice_no),
    INDEX idx_fact_date (invoice_date),
    INDEX idx_fact_stock (stock_code),
    INDEX idx_fact_customer (customer_id),
    INDEX idx_fact_country (country),
    INDEX idx_fact_cancellation (is_cancellation),
    INDEX idx_fact_year_month (transaction_year, transaction_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------------
-- 4. Pipeline Execution Telemetry & Audit Logs (etl_pipeline_logs)
-- Tracks run durations, row counts, and health status
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_pipeline_logs (
    run_id VARCHAR(100) NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NULL,
    status VARCHAR(50) NOT NULL,
    records_processed INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    error_message TEXT NULL,
    metrics_json JSON NULL,
    PRIMARY KEY (run_id),
    INDEX idx_log_status (pipeline_name, status),
    INDEX idx_log_start (start_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
