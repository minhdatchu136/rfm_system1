"""
etl/db.py — Tiện ích kết nối PostgreSQL
Dùng SQLAlchemy engine, hỗ trợ context manager.
"""

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import pandas as pd
import logging
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DB_URL

logger = logging.getLogger(__name__)


def get_engine(poolclass=None):
    """Tạo SQLAlchemy engine kết nối PostgreSQL."""
    kwargs = {"echo": False}
    if poolclass:
        kwargs["poolclass"] = poolclass
    engine = create_engine(DB_URL, **kwargs)
    return engine


def get_no_pool_engine():
    """Engine không dùng connection pool — dùng trong Prefect tasks."""
    return get_engine(poolclass=NullPool)


def test_connection() -> bool:
    """Kiểm tra kết nối có hoạt động không."""
    try:
        engine = get_no_pool_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info(f"[DB] Kết nối PostgreSQL thành công: {DB_URL.split('@')[1]}")
        return True
    except Exception as e:
        logger.error(f"[DB] Kết nối thất bại: {e}")
        return False


def execute_sql(sql: str, params: dict = None) -> None:
    """Thực thi câu SQL không trả về kết quả."""
    engine = get_no_pool_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def read_sql(query: str, params: dict = None, **kwargs) -> pd.DataFrame:
    """Đọc dữ liệu từ PostgreSQL về DataFrame."""
    from sqlalchemy import text
    engine = get_no_pool_engine()
    with engine.connect() as conn:
        # Thực thi query
        result = conn.execute(text(query), params or {})
        # Lấy tên cột
        columns = result.keys()
        # Lấy dữ liệu
        data = result.fetchall()
        # Chuyển thành DataFrame
        df = pd.DataFrame(data, columns=columns)
        return df


def upsert_df(df: pd.DataFrame, table: str,
              conflict_cols: list[str],
              update_cols: list[str] = None) -> int:
    """
    Upsert DataFrame vào bảng PostgreSQL.
    Dùng INSERT ... ON CONFLICT DO UPDATE.
    Trả về số dòng được xử lý.
    """
    if df.empty:
        return 0

    engine = get_no_pool_engine()

    # Chuyển kiểu dữ liệu
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    #for col in df.select_dtypes(include=["bool"]).columns:
        #df[col] = df[col].astype(int)

    cols         = df.columns.tolist()
    col_names    = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    conflict     = ", ".join(conflict_cols)

    if update_cols:
        updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        sql = (f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
               f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}")
    else:
        sql = (f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
               f"ON CONFLICT ({conflict}) DO NOTHING")

    with engine.begin() as conn:
        conn.execute(text(sql), df.to_dict("records"))

    return len(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ok = test_connection()
    print("Kết nối OK" if ok else "Kết nối THẤT BẠI")
