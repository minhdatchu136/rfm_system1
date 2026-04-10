"""
etl/load.py — Nạp dữ liệu vào PostgreSQL Data Warehouse
Dùng upsert để an toàn khi chạy lại (idempotent).
"""

import pandas as pd
import logging
import os, sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from etl.db import get_no_pool_engine, upsert_df, execute_sql

logger = logging.getLogger(__name__)


def load_dim_customer(df: pd.DataFrame) -> int:
    """Upsert DimCustomer — cập nhật last_order và total_orders nếu đã có."""
    return upsert_df(
        df, "dim_customer",
        conflict_cols=["customer_id"],
        update_cols=["country", "first_order", "last_order",
                     "total_orders", "updated_at"],
    )


def load_dim_product(df: pd.DataFrame) -> int:
    return upsert_df(df, "dim_product",
                     conflict_cols=["stock_code"],
                     update_cols=["description"])


def load_dim_date(df: pd.DataFrame) -> int:
    return upsert_df(df, "dim_date",
                     conflict_cols=["date_key"])


def load_dim_geography(df: pd.DataFrame) -> int:
    return upsert_df(df, "dim_geography",
                     conflict_cols=["country"])


def load_fact_sales(fact_staging: pd.DataFrame) -> int:
    """
    Nạp FactSales từ staging DataFrame vào PostgreSQL.
    Tra cứu surrogate key từ Dim tables rồi INSERT.
    """
    engine = get_no_pool_engine()
    from sqlalchemy import text

    # Đọc key maps từ DB
    with engine.connect() as conn:
        # Customer mapping
        result = conn.execute(text("SELECT customer_id, customer_key FROM dim_customer"))
        cust_map = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Product mapping
        result = conn.execute(text("SELECT stock_code, product_key FROM dim_product"))
        prod_map = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Geography mapping
        result = conn.execute(text("SELECT country, geo_key FROM dim_geography"))
        geo_map = pd.DataFrame(result.fetchall(), columns=result.keys())

    fact = fact_staging.copy()
    fact["customer_id"] = fact["customer_id"].astype(str)

    fact = fact.merge(cust_map, on="customer_id", how="left")
    fact = fact.merge(prod_map, on="stock_code",  how="left")
    fact = fact.merge(geo_map,  on="country",     how="left")

    fact_final = fact[[
        "invoice_no", "customer_key", "product_key",
        "date_key", "geo_key",
        "quantity", "unit_price", "total_amount", "invoice_date",
    ]].copy()

    # Xóa dòng thiếu key
    before = len(fact_final)
    fact_final = fact_final.dropna(subset=["customer_key", "product_key", "date_key"])
    if before != len(fact_final):
        logger.warning(f"[LOAD] Bỏ {before - len(fact_final)} dòng thiếu FK")

    # Chuyển kiểu dữ liệu
    fact_final["customer_key"] = fact_final["customer_key"].astype(int)
    fact_final["product_key"] = fact_final["product_key"].astype(int)
    fact_final["date_key"] = fact_final["date_key"].astype(int)
    fact_final["geo_key"] = fact_final["geo_key"].fillna(0).astype(int)
    fact_final["quantity"] = fact_final["quantity"].astype(int)
    fact_final["unit_price"] = fact_final["unit_price"].astype(float)
    fact_final["total_amount"] = fact_final["total_amount"].astype(float)
    fact_final["invoice_date"] = pd.to_datetime(fact_final["invoice_date"])

    # Batch insert
    BATCH = 5000
    total = 0
    
    insert_sql = text("""
        INSERT INTO fact_sales 
        (invoice_no, customer_key, product_key, date_key, geo_key, 
         quantity, unit_price, total_amount, invoice_date)
        VALUES (:invoice_no, :customer_key, :product_key, :date_key, :geo_key,
                :quantity, :unit_price, :total_amount, :invoice_date)
    """)
    
    with engine.begin() as conn:
        for i in range(0, len(fact_final), BATCH):
            batch = fact_final.iloc[i:i+BATCH]
            records = batch.to_dict('records')
            conn.execute(insert_sql, records)
            total += len(batch)
            if total % 50000 == 0:
                logger.info(f"[LOAD] Đã insert {total:,} / {len(fact_final):,} rows")

    logger.info(f"[LOAD] fact_sales: {total:,} rows loaded")
    return total


def log_pipeline_run(period: str, flow_name: str, status: str,
                     rows_loaded: int, started_at: datetime,
                     error_msg: str = None) -> None:
    """Ghi log mỗi lần chạy pipeline."""
    from sqlalchemy import text
    engine = get_no_pool_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_log
                (period, flow_name, status, rows_loaded, started_at, finished_at, error_msg)
            VALUES
                (:period, :flow_name, :status, :rows_loaded, :started_at, NOW(), :error_msg)
        """), {
            "period":      period,
            "flow_name":   flow_name,
            "status":      status,
            "rows_loaded": rows_loaded,
            "started_at":  started_at,
            "error_msg":   error_msg,
        })


def truncate_fact_for_period(year: int, month: int) -> None:
    """
    Xóa fact_sales cũ nếu cần chạy lại cho một tháng cụ thể.
    Tránh duplicate khi re-run.
    """
    execute_sql("""
        DELETE FROM fact_sales
        WHERE date_key >= :start AND date_key <= :end
    """, {
        "start": int(f"{year}{month:02d}01"),
        "end"  : int(f"{year}{month:02d}31"),
    })
    logger.info(f"[LOAD] Đã xóa fact_sales của {year}-{month:02d}")