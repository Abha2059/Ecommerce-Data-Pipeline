-- =====================================================================
-- HISTORICAL E-COMMERCE DATA WAREHOUSE: ANALYTICAL QUERIES
-- Database: ecommerce_dw (MySQL 8.0+)
-- Source: UCI Online Retail Historical Dataset (2010 - 2011)
-- =====================================================================

USE ecommerce_dw;

-- ---------------------------------------------------------------------
-- 1. Total Historical Revenue and Overall KPI Summary
-- Calculates total gross revenue, net units sold, total transactions, and cancellations
-- ---------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT invoice_no) AS total_invoices,
    COUNT(*) AS total_line_items,
    SUM(CASE WHEN is_cancellation = 0 THEN quantity ELSE 0 END) AS units_sold,
    SUM(CASE WHEN is_cancellation = 1 THEN ABS(quantity) ELSE 0 END) AS units_returned,
    ROUND(SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END), 2) AS total_gross_sales_gbp,
    ROUND(SUM(CASE WHEN is_cancellation = 1 THEN ABS(gross_amount) ELSE 0 END), 2) AS total_returns_gbp,
    ROUND(SUM(gross_amount), 2) AS net_revenue_gbp
FROM fact_sales;

-- ---------------------------------------------------------------------
-- 2. Monthly Revenue and Order Volume Trends
-- Evaluates sales progression across the historical period (Dec 2010 - Dec 2011)
-- ---------------------------------------------------------------------
SELECT 
    transaction_year,
    transaction_month,
    COUNT(DISTINCT invoice_no) AS total_orders,
    SUM(quantity) AS net_items_sold,
    ROUND(SUM(gross_amount), 2) AS net_monthly_revenue_gbp,
    ROUND(SUM(SUM(gross_amount)) OVER (ORDER BY transaction_year, transaction_month), 2) AS cumulative_revenue_gbp
FROM fact_sales
WHERE is_cancellation = 0
GROUP BY transaction_year, transaction_month
ORDER BY transaction_year, transaction_month;

-- ---------------------------------------------------------------------
-- 3. Top 10 Best-Selling Products by Revenue
-- Employs Window Function DENSE_RANK()
-- ---------------------------------------------------------------------
SELECT 
    stock_code,
    COALESCE(description, 'UNKNOWN') AS description,
    SUM(quantity) AS units_sold,
    ROUND(SUM(gross_amount), 2) AS total_revenue_gbp,
    DENSE_RANK() OVER (ORDER BY SUM(gross_amount) DESC) AS revenue_rank
FROM fact_sales
WHERE is_cancellation = 0 AND unit_price > 0
GROUP BY stock_code, description
ORDER BY revenue_rank ASC
LIMIT 10;

-- ---------------------------------------------------------------------
-- 4. International Sales Distribution (Revenue by Country)
-- Identifies UK vs International wholesale distribution
-- ---------------------------------------------------------------------
WITH country_metrics AS (
    SELECT 
        country,
        COUNT(DISTINCT invoice_no) AS order_count,
        SUM(quantity) AS units_sold,
        ROUND(SUM(gross_amount), 2) AS total_revenue_gbp
    FROM fact_sales
    WHERE is_cancellation = 0
    GROUP BY country
)
SELECT 
    country,
    order_count,
    units_sold,
    total_revenue_gbp,
    ROUND(total_revenue_gbp * 100.0 / SUM(total_revenue_gbp) OVER (), 2) AS revenue_pct
FROM country_metrics
ORDER BY total_revenue_gbp DESC
LIMIT 15;

-- ---------------------------------------------------------------------
-- 5. Cancellation & Return Analysis
-- Evaluates return frequencies and lost revenue per product
-- ---------------------------------------------------------------------
SELECT 
    COUNT(DISTINCT CASE WHEN is_cancellation = 1 THEN invoice_no END) AS total_cancellations,
    ROUND(COUNT(DISTINCT CASE WHEN is_cancellation = 1 THEN invoice_no END) * 100.0 / COUNT(DISTINCT invoice_no), 2) AS cancellation_rate_pct,
    ROUND(SUM(CASE WHEN is_cancellation = 1 THEN ABS(gross_amount) ELSE 0 END), 2) AS total_refunded_value_gbp
FROM fact_sales;

-- ---------------------------------------------------------------------
-- 6. Top 10 Most Returned Products by Volume
-- ---------------------------------------------------------------------
SELECT 
    stock_code,
    COALESCE(description, 'UNKNOWN') AS description,
    SUM(ABS(quantity)) AS total_returned_units,
    ROUND(SUM(ABS(gross_amount)), 2) AS total_returned_value_gbp
FROM fact_sales
WHERE is_cancellation = 1
GROUP BY stock_code, description
ORDER BY total_returned_units DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- 7. High-Value Customer Analysis (Wholesale Clients)
-- Ranks registered customers by cumulative lifetime spend
-- ---------------------------------------------------------------------
SELECT 
    customer_id,
    country,
    total_orders,
    ROUND(total_spend, 2) AS lifetime_spend_gbp,
    first_order_date,
    last_order_date
FROM dim_customers
ORDER BY total_spend DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- 8. Average Order Value (AOV)
-- ---------------------------------------------------------------------
WITH invoice_totals AS (
    SELECT 
        invoice_no,
        SUM(gross_amount) AS invoice_total
    FROM fact_sales
    WHERE is_cancellation = 0
    GROUP BY invoice_no
)
SELECT 
    COUNT(*) AS completed_invoices,
    ROUND(AVG(invoice_total), 2) AS avg_order_value_gbp,
    ROUND(MIN(invoice_total), 2) AS min_order_value_gbp,
    ROUND(MAX(invoice_total), 2) AS max_order_value_gbp
FROM invoice_totals;

-- ---------------------------------------------------------------------
-- 9. Guest vs. Registered Customer Share
-- Evaluates sales percentage attributed to registered accounts vs guest orders
-- ---------------------------------------------------------------------
SELECT 
    CASE WHEN customer_id IS NULL THEN 'Guest Checkout' ELSE 'Registered Customer' END AS checkout_type,
    COUNT(DISTINCT invoice_no) AS distinct_invoices,
    COUNT(*) AS total_line_items,
    ROUND(SUM(gross_amount), 2) AS total_gross_sales_gbp,
    ROUND(SUM(gross_amount) * 100.0 / SUM(SUM(gross_amount)) OVER (), 2) AS revenue_share_pct
FROM fact_sales
WHERE is_cancellation = 0
GROUP BY CASE WHEN customer_id IS NULL THEN 'Guest Checkout' ELSE 'Registered Customer' END;

-- ---------------------------------------------------------------------
-- 10. Daily Ingestion & Pipeline Health
-- ---------------------------------------------------------------------
SELECT 
    run_id,
    pipeline_name,
    start_time,
    end_time,
    TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_seconds,
    status,
    records_processed,
    records_rejected,
    error_message
FROM etl_pipeline_logs
ORDER BY start_time DESC
LIMIT 10;
