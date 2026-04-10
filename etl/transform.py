"""
etl/transform.py — Làm sạch dữ liệu và xây dựng Dim / Fact tables
"""

import pandas as pd
import numpy as np
import logging
import os, sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (DATE_COLUMN, CUSTOMER_COL, INVOICE_COL,
                    PRODUCT_COL, DESC_COL, QTY_COL, PRICE_COL, COUNTRY_COL)

logger = logging.getLogger(__name__)


# ── Làm sạch ─────────────────────────────────────────────────────────────

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    logger.info(f"[TRANSFORM] Làm sạch: {before:,} dòng")

    df = df.dropna(subset=[CUSTOMER_COL])
    df[CUSTOMER_COL] = df[CUSTOMER_COL].astype(int).astype(str)
    df[DATE_COLUMN]  = pd.to_datetime(df[DATE_COLUMN], format="mixed")
    df[QTY_COL]      = pd.to_numeric(df[QTY_COL],   errors="coerce")
    df[PRICE_COL]    = pd.to_numeric(df[PRICE_COL], errors="coerce")
    df[INVOICE_COL]  = df[INVOICE_COL].astype(str)

    df = df[~df[INVOICE_COL].str.startswith("C")]
    df = df[(df[QTY_COL] > 0) & (df[PRICE_COL] > 0)]
    df["total_amount"] = (df[QTY_COL] * df[PRICE_COL]).round(2)

    if DESC_COL in df.columns:
        df[DESC_COL] = df[DESC_COL].astype(str).str.strip().str.upper()

    logger.info(
        f"[TRANSFORM] Sau làm sạch: {len(df):,} dòng "
        f"(loại {before-len(df):,} | {(before-len(df))/before*100:.1f}%)"
    )
    return df.reset_index(drop=True)


# ── Dim tables ────────────────────────────────────────────────────────────

def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    dim = (df.groupby(CUSTOMER_COL)
             .agg(
                 country     = (COUNTRY_COL,  lambda x: x.mode()[0] if len(x) else "Unknown"),
                 first_order = (DATE_COLUMN,   "min"),
                 last_order  = (DATE_COLUMN,   "max"),
                 total_orders= (INVOICE_COL,   "nunique"),
             )
             .reset_index()
             .rename(columns={CUSTOMER_COL: "customer_id"}))
    logger.info(f"[TRANSFORM] dim_customer: {len(dim):,} rows")
    return dim


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    cols = {PRODUCT_COL: "stock_code"}
    if DESC_COL in df.columns:
        cols[DESC_COL] = "description"
    dim = (df[list(cols.keys())]
             .drop_duplicates(subset=[PRODUCT_COL])
             .rename(columns=cols)
             .reset_index(drop=True))
    logger.info(f"[TRANSFORM] dim_product: {len(dim):,} rows")
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.date_range(df[DATE_COLUMN].min().date(),
                          df[DATE_COLUMN].max().date(), freq="D")
    dim = pd.DataFrame({
        "date_key"  : dates.strftime("%Y%m%d").astype(int),
        "full_date" : dates.date,
        "year"      : dates.year,
        "quarter"   : dates.quarter,
        "month"     : dates.month,
        "month_name": dates.strftime("%B"),
        "week"      : dates.isocalendar().week.astype(int),
        "day_of_week": dates.dayofweek,
        "day_name"  : dates.strftime("%A"),
        "is_weekend": (dates.dayofweek >= 5).astype(bool),
    })
    logger.info(f"[TRANSFORM] dim_date: {len(dim):,} rows")
    logger.info(f"[TRANSFORM] dim_date is_weekend dtype: {dim['is_weekend'].dtype}")
    return dim


def build_dim_geography(df: pd.DataFrame) -> pd.DataFrame:
    if COUNTRY_COL not in df.columns:
        return pd.DataFrame(columns=["country"])
    dim = (df[[COUNTRY_COL]]
             .drop_duplicates()
             .rename(columns={COUNTRY_COL: "country"})
             .reset_index(drop=True))
    logger.info(f"[TRANSFORM] dim_geography: {len(dim):,} rows")
    return dim


# ── Fact table ────────────────────────────────────────────────────────────

def build_fact_sales(df: pd.DataFrame,
                     dim_customer: pd.DataFrame,
                     dim_product:  pd.DataFrame,
                     dim_date:     pd.DataFrame,
                     dim_geo:      pd.DataFrame) -> pd.DataFrame:

    fact = df.copy()

    # Lookup customer_key từ DB (dim đã có key sau khi load)
    # Ở đây dùng merge với surrogate key
    fact = fact.rename(columns={
        CUSTOMER_COL: "customer_id",
        PRODUCT_COL:  "stock_code",
        INVOICE_COL:  "invoice_no",
        DATE_COLUMN:  "invoice_date",
        QTY_COL:      "quantity",
        PRICE_COL:    "unit_price",
    })

    fact["date_key"] = pd.to_datetime(fact["invoice_date"]).dt.strftime("%Y%m%d").astype(int)

    # Chỉ giữ cột cần thiết
    fact = fact[[
        "invoice_no", "customer_id", "stock_code",
        "date_key", COUNTRY_COL if COUNTRY_COL in fact.columns else "invoice_no",
        "quantity", "unit_price", "total_amount", "invoice_date",
    ]].copy()

    if COUNTRY_COL in df.columns:
        fact = fact.rename(columns={COUNTRY_COL: "country"})
    else:
        fact["country"] = "Unknown"

    logger.info(f"[TRANSFORM] fact_sales staging: {len(fact):,} rows")
    return fact
