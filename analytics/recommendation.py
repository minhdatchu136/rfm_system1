"""
analytics/recommendation.py — Gợi ý sản phẩm theo phân khúc
"""

import pandas as pd
import logging
import os, sys
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import TOP_N_RECOMMENDATIONS, SEGMENT_METRIC
from etl.db import read_sql, get_no_pool_engine, upsert_df
from sqlalchemy import text

logger = logging.getLogger(__name__)


def _load_sales_with_segment(cutoff_date: str = None) -> pd.DataFrame:
    where = f"AND f.invoice_date <= '{cutoff_date}'" if cutoff_date else ""
    return read_sql(f"""
        SELECT
            cr.customer_id,
            cr.segment,
            dp.stock_code,
            dp.description,
            f.quantity,
            f.total_amount,
            f.invoice_date
        FROM fact_sales f
        JOIN dim_customer dc ON f.customer_key = dc.customer_key
        JOIN dim_product  dp ON f.product_key  = dp.product_key
        JOIN customer_rfm cr ON dc.customer_id  = cr.customer_id
        WHERE f.total_amount > 0
        {where}
    """, parse_dates=["invoice_date"])


def _compute_scores(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["stock_code","description","score"])

    if metric == "revenue":
        scores = (df.groupby(["stock_code","description"])
                    .agg(score=("total_amount","sum")).reset_index())

    elif metric == "recency_weighted":
        ref = df["invoice_date"].max()
        df  = df.copy()
        df["days_ago"] = (ref - df["invoice_date"]).dt.days
        df["w"]        = 1 / (1 + df["days_ago"])
        scores = (df.groupby(["stock_code","description"])
                    .agg(score=("w","sum")).reset_index())
    else:
        scores = (df.groupby(["stock_code","description"])
                    .agg(score=("quantity","sum")).reset_index())

    if scores["score"].max() > 0:
        scores["score"] = (scores["score"] / scores["score"].max() * 100).round(2)

    return scores.sort_values("score", ascending=False)


def build_recommendations(period: str,
                           cutoff_date: str = None,
                           top_n: int = TOP_N_RECOMMENDATIONS) -> pd.DataFrame:
    sales = _load_sales_with_segment(cutoff_date)
    results = []

    for seg, metric in SEGMENT_METRIC.items():
        seg_df = sales[sales["segment"] == seg]
        if seg_df.empty:
            continue
        scores = _compute_scores(seg_df, metric)
        top    = scores.head(top_n).copy()
        top["segment"] = seg
        top["rank"]    = range(1, len(top) + 1)
        top["period"]  = period
        results.append(top)
        logger.info(f"[RECO] {seg}: {len(top)} recommendations")

    if not results:
        return pd.DataFrame()

    final = pd.concat(results, ignore_index=True)
    final = final[["period","segment","stock_code","description","score","rank"]]
    return final


def save_recommendations(reco_df: pd.DataFrame) -> int:
    if reco_df.empty:
        return 0
    n = upsert_df(reco_df, "product_recommendation",
                  conflict_cols=["period","segment","rank"],
                  update_cols=["stock_code","description","score"])
    logger.info(f"[RECO] Saved {n} recommendations")
    return n
