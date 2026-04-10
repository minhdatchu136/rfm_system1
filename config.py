"""
config.py — Cấu hình trung tâm hệ thống RFM v2
Đọc từ biến môi trường; fallback về giá trị mặc định cho local dev.
"""

import os
from pathlib import Path

# ── Đường dẫn ──────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
SOURCE_FILE = DATA_DIR / "data.csv"

# ── PostgreSQL ─────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "rfm_warehouse")
DB_USER     = os.getenv("DB_USER",     "rfm_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "rfm_secret")

DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── Prefect ────────────────────────────────────────────────────────────────
PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect:4200/api")

# ── Tham số ETL ────────────────────────────────────────────────────────────
DATE_COLUMN  = "InvoiceDate"
CUSTOMER_COL = "CustomerID"
INVOICE_COL  = "InvoiceNo"
PRODUCT_COL  = "StockCode"
DESC_COL     = "Description"
QTY_COL      = "Quantity"
PRICE_COL    = "UnitPrice"
COUNTRY_COL  = "Country"

# ── Simulation: phân chia 13 tháng ────────────────────────────────────────
# Mỗi tháng tương ứng 1 lần chạy pipeline
# Format: (year, month)
SIMULATION_MONTHS = [
    (2010, 12),
    (2011,  1), (2011,  2), (2011,  3), (2011,  4),
    (2011,  5), (2011,  6), (2011,  7), (2011,  8),
    (2011,  9), (2011, 10), (2011, 11), (2011, 12),
]

# ── Tham số RFM ────────────────────────────────────────────────────────────
N_QUINTILES = 5

# ── Segment rules (R_min, R_max, F_min, F_max, M_min, M_max) ──────────────
SEGMENT_RULES = [
    ("Champions",                   5, 5, 1, 5, 5, 5),
    ("Champions",                   4, 5, 2, 5, 4, 5),
    ("Loyal Customers",             3, 5, 2, 5, 3, 5),
    ("Loyal Customers",             4, 5, 1, 5, 4, 5),
    ("Potential Loyalist",          4, 5, 1, 2, 3, 4),
    ("Potential Loyalist",          3, 4, 1, 2, 3, 4),
    ("Recent Customers",            5, 5, 1, 1, 1, 2),
    ("Promising",                   3, 4, 1, 1, 1, 2),
    ("Customers Needing Attention", 2, 3, 1, 5, 3, 4),
    ("About to Sleep",              2, 3, 1, 2, 1, 2),
    ("At Risk",                     1, 2, 2, 5, 3, 5),
    ("At Risk",                     1, 2, 1, 5, 5, 5),
    ("Hibernating",                 1, 2, 1, 5, 1, 5),
]

# ── Màu phân khúc ─────────────────────────────────────────────────────────
SEGMENT_COLORS = {
    "Champions":                   "#0369A1",
    "Loyal Customers":             "#0891B2",
    "Potential Loyalist":          "#0D9488",
    "Recent Customers":            "#059669",
    "Promising":                   "#65A30D",
    "Customers Needing Attention": "#D97706",
    "About to Sleep":              "#EA580C",
    "At Risk":                     "#DC2626",
    "Hibernating":                 "#6B7280",
}

# ── Recommendation ────────────────────────────────────────────────────────
TOP_N_RECOMMENDATIONS = 10

SEGMENT_METRIC = {
    "Champions":                   "revenue",
    "Loyal Customers":             "frequency",
    "Potential Loyalist":          "frequency",
    "Recent Customers":            "frequency",
    "Promising":                   "frequency",
    "Customers Needing Attention": "recency_weighted",
    "About to Sleep":              "recency_weighted",
    "At Risk":                     "revenue",
    "Hibernating":                 "frequency",
}
