-- ============================================================
-- database/init.sql
-- Schema PostgreSQL cho RFM Data Warehouse v2
-- Chạy tự động khi PostgreSQL container khởi động lần đầu
-- ============================================================

-- ── Extensions ───────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Dimension Tables ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key  SERIAL       PRIMARY KEY,
    customer_id   VARCHAR(20)  NOT NULL UNIQUE,
    country       VARCHAR(100),
    first_order   TIMESTAMP,
    last_order    TIMESTAMP,
    total_orders  INTEGER      DEFAULT 0,
    created_at    TIMESTAMP    DEFAULT NOW(),
    updated_at    TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key   SERIAL       PRIMARY KEY,
    stock_code    VARCHAR(20)  NOT NULL UNIQUE,
    description   TEXT,
    created_at    TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER      PRIMARY KEY,   -- YYYYMMDD
    full_date     DATE         NOT NULL,
    year          SMALLINT,
    quarter       SMALLINT,
    month         SMALLINT,
    month_name    VARCHAR(20),
    week          SMALLINT,
    day_of_week   SMALLINT,
    day_name      VARCHAR(20),
    is_weekend    BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geo_key       SERIAL       PRIMARY KEY,
    country       VARCHAR(100) NOT NULL UNIQUE
);

-- ── Fact Table ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_key      BIGSERIAL    PRIMARY KEY,
    invoice_no    VARCHAR(20),
    customer_key  INTEGER      REFERENCES dim_customer(customer_key),
    product_key   INTEGER      REFERENCES dim_product(product_key),
    date_key      INTEGER      REFERENCES dim_date(date_key),
    geo_key       INTEGER      REFERENCES dim_geography(geo_key),
    quantity      INTEGER,
    unit_price    NUMERIC(12,4),
    total_amount  NUMERIC(14,2),
    invoice_date  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_invoice  ON fact_sales(invoice_no);

-- ── RFM Latest (bảng kết quả mới nhất) ───────────────────────────────────

CREATE TABLE IF NOT EXISTS customer_rfm (
    customer_id    VARCHAR(20)  PRIMARY KEY,
    recency        INTEGER,
    frequency      INTEGER,
    monetary       NUMERIC(14,2),
    r_score        SMALLINT,
    f_score        SMALLINT,
    m_score        SMALLINT,
    rfm_score      VARCHAR(5),
    rfm_total      SMALLINT,
    segment        VARCHAR(50),
    calculated_at  TIMESTAMP    DEFAULT NOW()
);

-- ── RFM Snapshot (lịch sử theo tháng) ────────────────────────────────────

CREATE TABLE IF NOT EXISTS rfm_snapshot (
    snapshot_id    BIGSERIAL    PRIMARY KEY,
    period         VARCHAR(10)  NOT NULL,   -- '2011-03'
    snapshot_date  DATE         NOT NULL,
    customer_id    VARCHAR(20)  NOT NULL,
    recency        INTEGER,
    frequency      INTEGER,
    monetary       NUMERIC(14,2),
    r_score        SMALLINT,
    f_score        SMALLINT,
    m_score        SMALLINT,
    rfm_score      VARCHAR(5),
    segment        VARCHAR(50),
    UNIQUE (period, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_period   ON rfm_snapshot(period);
CREATE INDEX IF NOT EXISTS idx_snapshot_segment  ON rfm_snapshot(segment);
CREATE INDEX IF NOT EXISTS idx_snapshot_customer ON rfm_snapshot(customer_id);

-- ── Product Recommendation ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS product_recommendation (
    reco_id      BIGSERIAL    PRIMARY KEY,
    period       VARCHAR(10)  NOT NULL,
    segment      VARCHAR(50)  NOT NULL,
    stock_code   VARCHAR(20),
    description  TEXT,
    score        NUMERIC(8,2),
    rank         SMALLINT,
    UNIQUE (period, segment, rank)
);

CREATE INDEX IF NOT EXISTS idx_reco_period_seg ON product_recommendation(period, segment);

-- ── Pipeline Run Log ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_log (
    run_id       BIGSERIAL    PRIMARY KEY,
    period       VARCHAR(10),
    flow_name    VARCHAR(100),
    status       VARCHAR(20),  -- 'success' | 'failed'
    rows_loaded  INTEGER,
    started_at   TIMESTAMP,
    finished_at  TIMESTAMP,
    error_msg    TEXT
);

-- ── Views tiện ích ────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW vw_segment_monthly AS
SELECT
    period,
    segment,
    COUNT(*)                          AS customer_count,
    ROUND(AVG(recency)::numeric,1)    AS avg_recency,
    ROUND(AVG(frequency)::numeric,1)  AS avg_frequency,
    ROUND(AVG(monetary)::numeric,2)   AS avg_monetary,
    ROUND(SUM(monetary)::numeric,2)   AS total_revenue
FROM rfm_snapshot
GROUP BY period, segment
ORDER BY period, total_revenue DESC;

CREATE OR REPLACE VIEW vw_segment_migration AS
SELECT
    a.period          AS period_from,
    b.period          AS period_to,
    a.segment         AS segment_from,
    b.segment         AS segment_to,
    COUNT(*)          AS customer_count
FROM rfm_snapshot a
JOIN rfm_snapshot b
    ON  a.customer_id = b.customer_id
    AND b.period = (
        SELECT MIN(c.period)
        FROM rfm_snapshot c
        WHERE c.customer_id = a.customer_id
          AND c.period > a.period
    )
WHERE a.segment <> b.segment
GROUP BY 1,2,3,4
ORDER BY 1,2,5 DESC;
