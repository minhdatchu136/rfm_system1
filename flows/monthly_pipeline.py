"""
flows/monthly_pipeline.py — Prefect flow: ETL → RFM → Recommendation
Mỗi lần chạy xử lý 1 tháng dữ liệu (simulation 13 tháng).
"""

import pandas as pd
import logging
import os, sys
from datetime import datetime, date
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import SOURCE_FILE, SIMULATION_MONTHS
from etl.extract  import extract_data, validate_schema, filter_by_month
from etl.transform import (clean_data, build_dim_customer, build_dim_product,
                            build_dim_date, build_dim_geography, build_fact_sales)
from etl.load     import (load_dim_customer, load_dim_product, load_dim_date,
                           load_dim_geography, load_fact_sales, log_pipeline_run)
from analytics.rfm            import run_rfm_pipeline
from analytics.recommendation import build_recommendations, save_recommendations


# ══════════════════════════════════════════════════════════════════════════
# Tasks
# ══════════════════════════════════════════════════════════════════════════

@task(name="extract-raw-data", retries=2, retry_delay_seconds=30)
def task_extract(year: int, month: int) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"[EXTRACT] Loading data for period {year}-{month:02d}")
    raw = extract_data(SOURCE_FILE)
    validate_schema(raw)
    # Lọc tích lũy đến cuối tháng này
    filtered = filter_by_month(raw, year, month)
    logger.info(f"[EXTRACT] {len(filtered):,} rows after filtering")
    return filtered


@task(name="transform-data", retries=1)
def task_transform(raw: pd.DataFrame) -> dict:
    logger = get_run_logger()
    logger.info("[TRANSFORM] Cleaning and building Dim/Fact tables")
    cleaned  = clean_data(raw)
    dim_cust = build_dim_customer(cleaned)
    dim_prod = build_dim_product(cleaned)
    dim_date = build_dim_date(cleaned)
    dim_geo  = build_dim_geography(cleaned)
    fact_stg = build_fact_sales(cleaned, dim_cust, dim_prod, dim_date, dim_geo)
    return {
        "dim_customer"  : dim_cust,
        "dim_product"   : dim_prod,
        "dim_date"      : dim_date,
        "dim_geography" : dim_geo,
        "fact_staging"  : fact_stg,
    }


@task(name="load-to-warehouse", retries=2, retry_delay_seconds=60)
def task_load(tables: dict) -> int:
    logger = get_run_logger()
    logger.info("[LOAD] Writing to PostgreSQL Data Warehouse")

    n1 = load_dim_customer(tables["dim_customer"])
    n2 = load_dim_product(tables["dim_product"])
    n3 = load_dim_date(tables["dim_date"])
    n4 = load_dim_geography(tables["dim_geography"])
    n5 = load_fact_sales(tables["fact_staging"])

    logger.info(f"[LOAD] DimCustomer={n1}, DimProduct={n2}, "
                f"DimDate={n3}, DimGeo={n4}, Fact={n5}")
    return n5


@task(name="compute-rfm", retries=1)
def task_rfm(period: str, cutoff_date: str) -> int:
    logger = get_run_logger()
    logger.info(f"[RFM] Computing RFM snapshot for period {period}")
    rfm = run_rfm_pipeline(period, cutoff_date)
    logger.info(f"[RFM] {len(rfm):,} customers segmented")
    return len(rfm)


@task(name="build-recommendations", retries=1)
def task_recommendations(period: str, cutoff_date: str) -> int:
    logger = get_run_logger()
    logger.info(f"[RECO] Building recommendations for period {period}")
    reco = build_recommendations(period, cutoff_date)
    n    = save_recommendations(reco)
    logger.info(f"[RECO] {n} recommendations saved")
    return n


# ══════════════════════════════════════════════════════════════════════════
# Main Flow — 1 tháng
# ══════════════════════════════════════════════════════════════════════════

@flow(
    name="rfm-monthly-pipeline",
    description="ETL → DWH → RFM snapshot → Recommendation cho 1 tháng",
    log_prints=True,
)
def monthly_pipeline(year: int, month: int) -> dict:
    """
    Flow chính: xử lý toàn bộ pipeline cho tháng (year, month).
    Gọi từ simulation flow hoặc schedule trực tiếp.
    """
    logger  = get_run_logger()
    period  = f"{year}-{month:02d}"
    # Ngày cuối tháng làm cutoff
    cutoff  = (pd.Period(period, freq="M").end_time.date().strftime("%Y-%m-%d"))
    started = datetime.now()

    logger.info(f"{'='*50}")
    logger.info(f"  RFM Monthly Pipeline: {period} (cutoff={cutoff})")
    logger.info(f"{'='*50}")

    rows_loaded = 0
    try:
        raw    = task_extract(year, month)
        tables = task_transform(raw)
        rows_loaded = task_load(tables)
        task_rfm(period, cutoff)
        task_recommendations(period, cutoff)

        log_pipeline_run(period, "rfm-monthly-pipeline",
                         "success", rows_loaded, started)
        logger.info(f"[DONE] Period {period} completed successfully")

    except Exception as e:
        log_pipeline_run(period, "rfm-monthly-pipeline",
                         "failed", rows_loaded, started, str(e))
        logger.error(f"[ERROR] Period {period} failed: {e}")
        raise

    return {"period": period, "rows_loaded": rows_loaded, "status": "success"}


# ══════════════════════════════════════════════════════════════════════════
# Simulation Flow — chạy tất cả 13 tháng tuần tự
# ══════════════════════════════════════════════════════════════════════════

@flow(
    name="rfm-full-simulation",
    description="Giả lập 13 tháng dữ liệu, mỗi tháng 1 lần chạy pipeline",
    log_prints=True,
)
def full_simulation() -> list:
    """
    Chạy pipeline tuần tự cho tất cả SIMULATION_MONTHS.
    Mỗi tháng thêm dữ liệu tích lũy → snapshot RFM → gợi ý sản phẩm.
    Dùng để khởi tạo toàn bộ lịch sử cho dashboard.
    """
    logger = get_run_logger()
    logger.info(f"Starting full simulation: {len(SIMULATION_MONTHS)} months")

    results = []
    for year, month in SIMULATION_MONTHS:
        logger.info(f"\n>>> Processing {year}-{month:02d}...")
        result = monthly_pipeline(year, month)
        results.append(result)
        logger.info(f"<<< {year}-{month:02d} done: {result}")

    logger.info(f"\n{'='*50}")
    logger.info(f"  SIMULATION COMPLETE: {len(results)} periods processed")
    logger.info(f"{'='*50}")
    return results


# ══════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "simulate":
        full_simulation()
    elif len(sys.argv) == 3:
        y, m = int(sys.argv[1]), int(sys.argv[2])
        monthly_pipeline(y, m)
    else:
        print("Usage:")
        print("  python flows/monthly_pipeline.py simulate       # chạy tất cả 13 tháng")
        print("  python flows/monthly_pipeline.py 2011 6         # chạy tháng cụ thể")
