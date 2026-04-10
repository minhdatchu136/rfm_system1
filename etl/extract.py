"""
etl/extract.py — Đọc dữ liệu nguồn (CSV / Excel)
"""

import pandas as pd
import logging
import os, sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import SOURCE_FILE, DATE_COLUMN, CUSTOMER_COL, INVOICE_COL, QTY_COL, PRICE_COL

logger = logging.getLogger(__name__)


def extract_data(filepath=None) -> pd.DataFrame:
    """Đọc file nguồn, trả về DataFrame thô."""
    fp  = Path(filepath or SOURCE_FILE)
    ext = fp.suffix.lower()
    logger.info(f"[EXTRACT] Đọc: {fp.name}  ({ext})")

    if ext == ".csv":
        try:
            df = pd.read_csv(fp, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="latin-1", low_memory=False)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(fp)
    else:
        raise ValueError(f"Định dạng không hỗ trợ: {ext}")

    logger.info(f"[EXTRACT] {len(df):,} dòng × {df.shape[1]} cột")
    return df


def validate_schema(df: pd.DataFrame) -> None:
    required = [DATE_COLUMN, CUSTOMER_COL, INVOICE_COL, QTY_COL, PRICE_COL]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc: {missing}")
    logger.info("[EXTRACT] Schema hợp lệ ✓")


def filter_by_month(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    Lọc dữ liệu tích lũy đến cuối tháng (year, month).
    Dùng cho simulation: mỗi lần chạy pipeline thêm 1 tháng dữ liệu mới.
    """
    import pandas as pd
    df = df.copy()
    df[DATE_COLUMN] = pd.to_datetime(df[DATE_COLUMN], format="mixed")

    cutoff = pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)
    result = df[df[DATE_COLUMN] <= cutoff]

    logger.info(
        f"[EXTRACT] Lọc đến {cutoff.date()}: "
        f"{len(result):,}/{len(df):,} dòng ({len(result)/len(df)*100:.1f}%)"
    )
    return result
