"""
Data Service Layer for E-Commerce Sales Analytics Dashboard
Supports Dual-Engine execution:
1. MySQL 8.0 Data Warehouse (Local development)
2. AWS S3 Parquet Lakehouse via DuckDB (Serverless Cloud Deployment)
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
import sqlalchemy
from sqlalchemy import text

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

from src.utils.config import cfg
from src.utils.logger_config import get_logger

logger = get_logger("app.data_service")


class DataService:
    def __init__(self):
        self.data_source = os.getenv("DATA_SOURCE", "auto")
        self.s3_bucket = os.getenv("AWS_S3_BUCKET", "ecommerce-data-pipeline-abhay-699258776334")
        self.s3_region = os.getenv("AWS_REGION", "ap-south-1")
        self.use_s3 = False
        self.duck_conn = None

        if self.data_source == "s3":
            self._init_duckdb_s3()
        else:
            try:
                self.engine = sqlalchemy.create_engine(
                    cfg.db.connection_url,
                    pool_recycle=3600,
                    pool_pre_ping=True,
                    echo=False,
                )
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1;"))
                logger.info("Connected to local MySQL Data Warehouse.")
            except Exception as e:
                logger.warning(f"Local MySQL unavailable ({e}). Initializing AWS S3 Parquet Lakehouse via DuckDB...")
                self._init_duckdb_s3()

    def _init_duckdb_s3(self):
        """Initializes in-memory DuckDB engine querying AWS S3 Parquet Lakehouse."""
        import duckdb
        logger.info(f"Connecting to AWS S3 bucket: s3://{self.s3_bucket} (region: {self.s3_region})...")
        self.duck_conn = duckdb.connect()
        self.duck_conn.execute("INSTALL httpfs; LOAD httpfs;")
        self.duck_conn.execute("INSTALL aws; LOAD aws;")

        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if aws_key and aws_secret:
            self.duck_conn.execute(f"SET s3_region = '{self.s3_region}';")
            self.duck_conn.execute(f"SET s3_access_key_id = '{aws_key}';")
            self.duck_conn.execute(f"SET s3_secret_access_key = '{aws_secret}';")
        else:
            self.duck_conn.execute("CALL load_aws_credentials();")

        # Load S3 Parquet partitions into high-speed in-memory tables
        s3_glob = f"s3://{self.s3_bucket}/processed/historical_sales/*/*/*.parquet"
        self.duck_conn.execute(f"""
            CREATE TABLE IF NOT EXISTS fact_sales AS 
            SELECT * FROM '{s3_glob}';
        """)

        self.duck_conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_products AS 
            SELECT 
                stock_code,
                MAX(description) AS description,
                SUM(CASE WHEN is_cancellation = 0 THEN quantity ELSE 0 END) AS total_units_sold,
                ROUND(AVG(unit_price), 2) AS latest_unit_price,
                MIN(invoice_date) AS first_sold_at,
                MAX(invoice_date) AS last_sold_at
            FROM fact_sales
            GROUP BY stock_code;
        """)

        self.duck_conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_customers AS 
            SELECT 
                customer_id,
                MAX(country) AS country,
                COUNT(DISTINCT invoice_no) AS total_orders,
                ROUND(SUM(gross_amount), 2) AS total_spend,
                MIN(invoice_date) AS first_order_date,
                MAX(invoice_date) AS last_order_date
            FROM fact_sales
            WHERE customer_id IS NOT NULL AND is_cancellation = 0
            GROUP BY customer_id;
        """)

        self.use_s3 = True
        logger.info("AWS S3 Lakehouse successfully initialized in DuckDB.")

    def _execute_query(self, sql_str: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Executes query either against MySQL engine or DuckDB S3 engine."""
        if self.use_s3:
            # DuckDB parameters using ? or direct execution
            if params:
                for k, v in params.items():
                    if isinstance(v, str):
                        sql_str = sql_str.replace(f":{k}", f"'{v}'")
                    else:
                        sql_str = sql_str.replace(f":{k}", str(v))
            return self.duck_conn.execute(sql_str).df()
        else:
            with self.engine.connect() as conn:
                return pd.read_sql(text(sql_str), conn, params=params)

    def get_sales_kpis(self, year: Optional[int] = None, country_filter: Optional[str] = None) -> Dict[str, Any]:
        """Calculates executive sales KPIs with optional horizon and regional filters."""
        where_clauses = ["1=1"]
        params = {}
        if year:
            where_clauses.append("transaction_year = :year")
            params["year"] = year
        if country_filter == "UK":
            where_clauses.append("country = 'United Kingdom'")
        elif country_filter == "International":
            where_clauses.append("country != 'United Kingdom'")
        elif country_filter and country_filter != "Global":
            where_clauses.append("country = :country")
            params["country"] = country_filter

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                COUNT(*) AS total_records,
                COUNT(DISTINCT invoice_no) AS total_orders,
                COUNT(DISTINCT customer_id) AS active_customers,
                COALESCE(SUM(CASE WHEN is_cancellation = 0 THEN quantity ELSE 0 END), 0) AS units_sold,
                COALESCE(SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END), 0) AS gross_sales,
                COALESCE(SUM(CASE WHEN is_cancellation = 1 THEN gross_amount ELSE 0 END), 0) AS cancellation_amount,
                COALESCE(SUM(gross_amount), 0) AS net_sales,
                SUM(CASE WHEN is_cancellation = 1 THEN 1 ELSE 0 END) AS cancellation_records,
                COUNT(DISTINCT CASE WHEN is_cancellation = 1 THEN invoice_no ELSE NULL END) AS cancelled_orders,
                SUM(CASE WHEN customer_id IS NULL AND is_cancellation = 0 THEN 1 ELSE 0 END) AS guest_records,
                COALESCE(SUM(CASE WHEN customer_id IS NULL AND is_cancellation = 0 THEN gross_amount ELSE 0 END), 0) AS guest_sales
            FROM fact_sales
            WHERE {where_sql};
        """

        df = self._execute_query(query, params)
        row = df.iloc[0]

        total_records = int(row["total_records"] or 0)
        total_orders = int(row["total_orders"] or 0)
        active_customers = int(row["active_customers"] or 0)
        units_sold = int(row["units_sold"] or 0)
        gross_sales = float(row["gross_sales"] or 0.0)
        cancellation_amount = float(row["cancellation_amount"] or 0.0)
        net_sales = float(row["net_sales"] or 0.0)
        cancellation_records = int(row["cancellation_records"] or 0)
        cancelled_orders = int(row["cancelled_orders"] or 0)
        guest_records = int(row["guest_records"] or 0)
        guest_sales = float(row["guest_sales"] or 0.0)

        valid_orders = total_orders - cancelled_orders
        aov = (gross_sales / valid_orders) if valid_orders > 0 else 0.0
        return_rate_pct = (cancelled_orders / total_orders * 100) if total_orders > 0 else 0.0

        return {
            "total_records": total_records,
            "total_orders": total_orders,
            "valid_orders": valid_orders,
            "active_customers": active_customers,
            "units_sold": units_sold,
            "gross_sales": gross_sales,
            "cancellation_amount": cancellation_amount,
            "net_sales": net_sales,
            "cancellation_records": cancellation_records,
            "cancelled_orders": cancelled_orders,
            "aov": aov,
            "return_rate_pct": return_rate_pct,
            "guest_records": guest_records,
            "guest_sales": guest_sales,
        }

    def get_sales_trends(self, year: Optional[int] = None, country_filter: Optional[str] = None) -> pd.DataFrame:
        """Retrieves monthly sales progression."""
        where_clauses = ["1=1"]
        params = {}
        if year:
            where_clauses.append("transaction_year = :year")
            params["year"] = year
        if country_filter == "UK":
            where_clauses.append("country = 'United Kingdom'")
        elif country_filter == "International":
            where_clauses.append("country != 'United Kingdom'")
        elif country_filter and country_filter != "Global":
            where_clauses.append("country = :country")
            params["country"] = country_filter

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                transaction_year,
                transaction_month,
                CONCAT(transaction_year, '-', LPAD(CAST(transaction_month AS VARCHAR), 2, '0')) AS period_ym,
                COUNT(DISTINCT invoice_no) AS total_orders,
                SUM(CASE WHEN is_cancellation = 0 THEN quantity ELSE 0 END) AS units_sold,
                ROUND(SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END), 2) AS gross_sales,
                ROUND(SUM(CASE WHEN is_cancellation = 1 THEN ABS(gross_amount) ELSE 0 END), 2) AS refunds,
                ROUND(SUM(gross_amount), 2) AS net_sales,
                ROUND(
                    SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END) / 
                    NULLIF(COUNT(DISTINCT CASE WHEN is_cancellation = 0 THEN invoice_no END), 0), 2
                ) AS avg_order_value
            FROM fact_sales
            WHERE {where_sql}
            GROUP BY transaction_year, transaction_month
            ORDER BY transaction_year ASC, transaction_month ASC;
        """
        return self._execute_query(query, params)

    def get_sales_by_day_and_hour(self) -> Dict[str, pd.DataFrame]:
        """Retrieves shopping timing patterns."""
        day_query = """
            SELECT 
                DAYNAME(invoice_date) AS day_name,
                DAYOFWEEK(invoice_date) AS day_num,
                COUNT(DISTINCT invoice_no) AS order_count,
                ROUND(SUM(gross_amount), 2) AS total_sales
            FROM fact_sales
            WHERE is_cancellation = 0
            GROUP BY day_name, day_num
            ORDER BY day_num;
        """
        hour_query = """
            SELECT 
                HOUR(invoice_date) AS order_hour,
                COUNT(DISTINCT invoice_no) AS order_count,
                ROUND(SUM(gross_amount), 2) AS total_sales
            FROM fact_sales
            WHERE is_cancellation = 0
            GROUP BY order_hour
            ORDER BY order_hour;
        """
        return {
            "by_day": self._execute_query(day_query),
            "by_hour": self._execute_query(hour_query)
        }

    def get_top_products_sales(self, limit: int = 10, order_by: str = "revenue", country_filter: Optional[str] = None) -> pd.DataFrame:
        """Returns top performing merchandise."""
        where_clauses = ["is_cancellation = 0"]
        params = {"limit": int(limit)}

        if country_filter == "UK":
            where_clauses.append("country = 'United Kingdom'")
        elif country_filter == "International":
            where_clauses.append("country != 'United Kingdom'")
        elif country_filter and country_filter != "Global":
            where_clauses.append("country = :country")
            params["country"] = country_filter

        where_sql = " AND ".join(where_clauses)
        sort_col = "total_revenue" if order_by == "revenue" else "total_units"

        query = f"""
            SELECT 
                stock_code,
                description,
                SUM(quantity) AS total_units,
                ROUND(SUM(gross_amount), 2) AS total_revenue,
                ROUND(AVG(unit_price), 2) AS avg_unit_price
            FROM fact_sales
            WHERE {where_sql}
            GROUP BY stock_code, description
            ORDER BY {sort_col} DESC
            LIMIT :limit;
        """
        return self._execute_query(query, params)

    def get_product_price_tiers(self) -> pd.DataFrame:
        """Analyzes sales across price brackets."""
        query = """
            SELECT 
                CASE 
                    WHEN unit_price < 2.0 THEN 'Budget (< £2.00)'
                    WHEN unit_price BETWEEN 2.0 AND 5.0 THEN 'Standard (£2.00 - £5.00)'
                    WHEN unit_price BETWEEN 5.01 AND 15.0 THEN 'Premium (£5.01 - £15.00)'
                    ELSE 'Luxury / Bulk (> £15.00)'
                END AS price_tier,
                COUNT(*) AS transaction_lines,
                SUM(quantity) AS units_sold,
                ROUND(SUM(gross_amount), 2) AS total_revenue
            FROM fact_sales
            WHERE is_cancellation = 0
            GROUP BY price_tier
            ORDER BY total_revenue DESC;
        """
        return self._execute_query(query)

    def search_product(self, search_term: str) -> pd.DataFrame:
        """Searches product catalog from dim_products."""
        query = """
            SELECT 
                stock_code,
                description,
                total_units_sold AS total_units,
                latest_unit_price AS unit_price,
                ROUND(total_units_sold * latest_unit_price, 2) AS estimated_revenue,
                first_sold_at AS first_sale,
                last_sold_at AS latest_sale
            FROM dim_products
            WHERE stock_code LIKE :term OR description LIKE :term
            ORDER BY total_units_sold DESC
            LIMIT 25;
        """
        return self._execute_query(query, {"term": f"%{search_term}%"})

    def get_customer_segments(self) -> Dict[str, Any]:
        """Generates customer lifetime value tiers and VIP table."""
        tiers_query = """
            SELECT 
                CASE 
                    WHEN total_spend >= 10000 THEN 'VIP Tier (>£10k)'
                    WHEN total_spend BETWEEN 2000 AND 9999.99 THEN 'High-Value (£2k-£10k)'
                    WHEN total_spend BETWEEN 500 AND 1999.99 THEN 'Mid-Tier (£500-£2k)'
                    ELSE 'Budget (<£500)'
                END AS spend_tier,
                COUNT(*) AS customer_count,
                ROUND(SUM(total_spend), 2) AS tier_revenue,
                ROUND(AVG(total_orders), 1) AS avg_orders_per_customer
            FROM dim_customers
            GROUP BY spend_tier
            ORDER BY tier_revenue DESC;
        """
        vip_query = """
            SELECT 
                customer_id,
                country,
                total_orders,
                ROUND(total_spend, 2) AS total_spend,
                ROUND(total_spend / total_orders, 2) AS avg_order_value,
                first_order_date,
                last_order_date
            FROM dim_customers
            ORDER BY total_spend DESC
            LIMIT 10;
        """
        return {
            "tiers": self._execute_query(tiers_query),
            "top_vips": self._execute_query(vip_query)
        }

    def get_geographic_sales(self) -> pd.DataFrame:
        """Returns country-level sales matrix."""
        query = """
            SELECT 
                country,
                COUNT(DISTINCT invoice_no) AS total_orders,
                SUM(CASE WHEN is_cancellation = 0 THEN quantity ELSE 0 END) AS total_units,
                ROUND(SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END), 2) AS gross_sales,
                ROUND(SUM(gross_amount), 2) AS net_sales,
                ROUND(
                    SUM(CASE WHEN is_cancellation = 0 THEN gross_amount ELSE 0 END) / 
                    COUNT(DISTINCT CASE WHEN is_cancellation = 0 THEN invoice_no END), 2
                ) AS avg_order_value,
                COUNT(DISTINCT customer_id) AS distinct_customers
            FROM fact_sales
            GROUP BY country
            ORDER BY gross_sales DESC;
        """
        return self._execute_query(query)

    def search_orders(
        self,
        query_text: Optional[str] = None,
        country_filter: Optional[str] = None,
        status_filter: Optional[str] = None,
        limit: int = 50
    ) -> pd.DataFrame:
        """Search and filter transactional sales orders."""
        where_clauses = ["1=1"]
        params = {"limit": int(limit)}

        if query_text:
            where_clauses.append("(invoice_no LIKE :q OR stock_code LIKE :q OR description LIKE :q OR CAST(customer_id AS VARCHAR) LIKE :q)")
            params["q"] = f"%{query_text}%"

        if country_filter and country_filter != "Global":
            where_clauses.append("country = :country")
            params["country"] = country_filter

        if status_filter == "Completed Sales":
            where_clauses.append("is_cancellation = 0")
        elif status_filter == "Cancelled / Returned":
            where_clauses.append("is_cancellation = 1")

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT 
                invoice_no,
                invoice_date,
                customer_id,
                country,
                stock_code,
                description,
                quantity,
                unit_price,
                gross_amount,
                CASE WHEN is_cancellation = 1 THEN 'Returned' ELSE 'Completed' END AS order_status
            FROM fact_sales
            WHERE {where_sql}
            ORDER BY invoice_date DESC
            LIMIT :limit;
        """
        return self._execute_query(query, params)
