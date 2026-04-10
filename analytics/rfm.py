"""
analytics/rfm.py — Tính RFM và phân khúc, lưu snapshot hàng tháng
"""

import pandas as pd
import numpy as np
import logging
import os, sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import SEGMENT_RULES, N_QUINTILES
from etl.db import get_no_pool_engine, read_sql, upsert_df

logger = logging.getLogger(__name__)


# ── Load transactions ─────────────────────────────────────────────────────

def load_transactions(cutoff_date: str = None) -> pd.DataFrame:
    """
    Đọc giao dịch từ DWH.
    cutoff_date: 'YYYY-MM-DD' — nếu có, chỉ lấy đến ngày này.
    """
    where = ""
    if cutoff_date:
        where = f"WHERE f.invoice_date <= '{cutoff_date}'"

    df = read_sql(f"""
        SELECT
            dc.customer_id,
            f.invoice_no,
            f.invoice_date,
            f.total_amount
        FROM fact_sales f
        JOIN dim_customer dc ON f.customer_key = dc.customer_key
        WHERE f.total_amount > 0
        {("AND f.invoice_date <= '" + cutoff_date + "'") if cutoff_date else ""}
    """, parse_dates=["invoice_date"])
    
    # Chuyển đổi Decimal sang float
    if 'total_amount' in df.columns:
        df['total_amount'] = df['total_amount'].astype(float)
    
    logger.info(f"[RFM] Loaded {len(df):,} transactions (cutoff={cutoff_date})")
    return df


# ── Tính R, F, M ──────────────────────────────────────────────────────────

def compute_rfm(df: pd.DataFrame, reference_date=None) -> pd.DataFrame:
    if reference_date is None:
        reference_date = df["invoice_date"].max() + timedelta(days=1)
    if isinstance(reference_date, str):
        reference_date = pd.Timestamp(reference_date) + timedelta(days=1)

    logger.info(f"[RFM] Reference date: {pd.Timestamp(reference_date).date()}")

    rfm = (df.groupby("customer_id")
             .agg(
                 Recency   = ("invoice_date",  lambda x: (reference_date - x.max()).days),
                 Frequency = ("invoice_no",    "nunique"),
                 Monetary  = ("total_amount",  "sum"),
             )
             .reset_index())
    rfm["Monetary"] = rfm["Monetary"].round(2)
    logger.info(f"[RFM] Computed RFM for {len(rfm):,} customers")
    return rfm


# ── Chấm điểm quintile ────────────────────────────────────────────────────

def _score_column(series: pd.Series, n: int, reverse: bool = False) -> pd.Series:
    # Chuyển đổi Decimal sang float nếu cần
    if hasattr(series, 'dtype') and str(series.dtype).startswith('decimal'):
        series = series.astype(float)
    
    labels = list(range(1, n + 1))
    if reverse:
        labels = labels[::-1]
    try:
        return pd.qcut(series, q=n, labels=labels, duplicates="drop").astype(int)
    except Exception:
        return pd.cut(series, bins=n, labels=labels, duplicates="drop").astype(int)


def score_rfm(rfm: pd.DataFrame, n: int = N_QUINTILES) -> pd.DataFrame:
    rfm = rfm.copy()
    
    # Chuyển đổi tất cả các cột số từ Decimal sang float
    for col in ['Recency', 'Frequency', 'Monetary']:
        if col in rfm.columns:
            if hasattr(rfm[col], 'dtype') and 'decimal' in str(rfm[col].dtype):
                rfm[col] = rfm[col].astype(float)
    
    rfm["R_Score"] = _score_column(rfm["Recency"],   n, reverse=True)
    rfm["F_Score"] = _score_column(rfm["Frequency"], n, reverse=False)
    rfm["M_Score"] = _score_column(rfm["Monetary"],  n, reverse=False)
    rfm["RFM_Score"] = (rfm["R_Score"].astype(str)
                        + rfm["F_Score"].astype(str)
                        + rfm["M_Score"].astype(str))
    rfm["RFM_Total"] = rfm["R_Score"] + rfm["F_Score"] + rfm["M_Score"]
    return rfm


# ── Gán phân khúc ─────────────────────────────────────────────────────────

def assign_segment(rfm: pd.DataFrame) -> pd.DataFrame:
    rfm = rfm.copy()
    rfm["Segment"] = "Hibernating"

    for rule in SEGMENT_RULES:
        seg, r_min, r_max, f_min, f_max, m_min, m_max = rule
        mask = (
            rfm["Segment"].eq("Hibernating") &
            rfm["R_Score"].between(r_min, r_max) &
            rfm["F_Score"].between(f_min, f_max) &
            rfm["M_Score"].between(m_min, m_max)
        )
        rfm.loc[mask, "Segment"] = seg

    counts = rfm["Segment"].value_counts()
    logger.info(f"[RFM] Segments:\n{counts.to_string()}")
    return rfm


# ── Lưu kết quả ──────────────────────────────────────────────────────────

def save_rfm_latest(rfm: pd.DataFrame) -> int:
    """Ghi/cập nhật bảng customer_rfm (kết quả mới nhất)."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rfm_save = rfm.rename(columns={
        "customer_id": "customer_id",
        "Recency"    : "recency",
        "Frequency"  : "frequency",
        "Monetary"   : "monetary",
        "R_Score"    : "r_score",
        "F_Score"    : "f_score",
        "M_Score"    : "m_score",
        "RFM_Score"  : "rfm_score",
        "RFM_Total"  : "rfm_total",
        "Segment"    : "segment",
    }).copy()
    rfm_save["calculated_at"] = now

    n = upsert_df(rfm_save, "customer_rfm",
                  conflict_cols=["customer_id"],
                  update_cols=["recency","frequency","monetary",
                               "r_score","f_score","m_score",
                               "rfm_score","rfm_total","segment",
                               "calculated_at"])
    logger.info(f"[RFM] customer_rfm: {n:,} rows upserted")
    return n


def save_rfm_snapshot(rfm: pd.DataFrame, period: str) -> int:
    """
    Lưu snapshot RFM theo tháng vào rfm_snapshot.
    period: '2011-03' (YYYY-MM)
    """
    snapshot_date = pd.Period(period, freq="M").end_time.date().strftime("%Y-%m-%d")

    snap = rfm.rename(columns={
        "customer_id": "customer_id",
        "Recency"    : "recency",
        "Frequency"  : "frequency",
        "Monetary"   : "monetary",
        "R_Score"    : "r_score",
        "F_Score"    : "f_score",
        "M_Score"    : "m_score",
        "RFM_Score"  : "rfm_score",
        "Segment"    : "segment",
    }).copy()
    snap["period"]        = period
    snap["snapshot_date"] = snapshot_date

    cols = ["period","snapshot_date","customer_id","recency","frequency",
            "monetary","r_score","f_score","m_score","rfm_score","segment"]
    snap = snap[cols]

    n = upsert_df(snap, "rfm_snapshot",
                  conflict_cols=["period","customer_id"],
                  update_cols=["recency","frequency","monetary",
                               "r_score","f_score","m_score",
                               "rfm_score","segment"])
    logger.info(f"[RFM] rfm_snapshot [{period}]: {n:,} rows")
    return n


# ── Pipeline tổng hợp ─────────────────────────────────────────────────────

def run_rfm_pipeline(period: str, cutoff_date: str = None) -> pd.DataFrame:
    """
    Chạy toàn bộ RFM pipeline cho một kỳ (period).
    period   : '2011-03'
    cutoff_date: ngày cuối tháng, vd '2011-03-31'
    """
    df  = load_transactions(cutoff_date)
    rfm = compute_rfm(df, reference_date=cutoff_date)
    rfm = score_rfm(rfm)
    rfm = assign_segment(rfm)

    save_rfm_latest(rfm)
    save_rfm_snapshot(rfm, period)

    return rfm