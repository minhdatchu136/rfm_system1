# RFM Analytics System v2

Hệ thống phân tích khách hàng RFM — end-to-end: **PostgreSQL · Prefect · Docker · Streamlit**



---

## Kiến trúc hệ thống

```
                CSV / Excel
                    │
                    ▼
           ┌─────────────────┐
           │  Prefect Flow   │  lên lịch hàng tháng
           │  ETL pipeline   │  retry tự động
           └────────┬────────┘
                    │
                    ▼
       ┌────────────────────────┐
       │      PostgreSQL        │  thay SQLite
       │   Data Warehouse       │
       │                        │
       │  dim_customer          │
       │  dim_product           │
       │  dim_date              │
       │  dim_geography         │
       │  fact_sales            │
       │  customer_rfm          │  ← kết quả mới nhất
       │  rfm_snapshot          │  ← lịch sử 13 tháng ★
       │  product_recommendation│
       │  pipeline_log          │
       └────────────────────────┘
                    │
                    ▼
          ┌──────────────────┐
          │ Streamlit         │
          │                   │
          │ Home              │
          │ Business Overview │
          │ RFM Analysis      │
          │ Segment History   │  ← stacked area, migration, journey
          │ Recommendations   │
          └──────────────────┘
```

---

## Cấu trúc project

```
rfm_v2/
├── .env.example              ← template biến môi trường
├── .gitignore
├── docker-compose.yml        ← 5 services
├── Dockerfile
├── requirements.txt
├── config.py                 ← cấu hình trung tâm, 13 tháng simulation
│
├── database/
│   └── init.sql              ← PostgreSQL schema (tự chạy khi start)
│
├── etl/
│   ├── db.py                 ← SQLAlchemy engine, upsert helper
│   ├── extract.py            ← đọc file + filter_by_month()
│   ├── transform.py          ← làm sạch, Dim/Fact builders
│   └── load.py               ← upsert idempotent, pipeline_log
│
├── analytics/
│   ├── rfm.py                ← compute_rfm, score, segment, snapshot
│   └── recommendation.py    ← segment-based scoring, save
│
├── flows/
│   ├── monthly_pipeline.py   ← Prefect tasks + flow (1 tháng + simulation)
│   └── schedule.py           ← deploy lên Prefect server
│
├── dashboard/
│   ├── app.py                ← Trang chủ
│   ├── styles.py
│   └── pages/
│       ├── 1_Business_Overview.py
│       ├── 2_RFM_Analysis.py
│       ├── 3_Segment_History.py   ← MỚI
│       └── 4_Product_Recommendations.py
│
└── tests/
    └── test_pipeline.py      ← pytest: extract, transform, RFM logic
```

---

## Khởi động nhanh

### 1. Clone và cấu hình

```bash
git clone <repo-url> && cd rfm_v2

# Điền password vào .env
cp .env.example .env
# Sửa DB_PASSWORD trong .env
```

### 2. Thêm file dữ liệu

```bash
# Đặt file CSV vào thư mục data/
cp /path/to/data.csv data/data.csv
```

### 3. Khởi động tất cả services

```bash
docker compose up -d

# Kiểm tra tất cả đã ready
docker compose ps
```

Services sẽ khởi động:
| Service | URL | Mô tả |
|---|---|---|
| `postgres` | `localhost:5432` | PostgreSQL DWH |
| `prefect-server` | `http://localhost:4200` | Prefect UI |
| `prefect-worker` | — | Chạy flows |
| `dashboard` | `http://localhost:8501` | Streamlit |

### 4. Chạy simulation 13 tháng

```bash
# Chạy toàn bộ 13 tháng lịch sử (chỉ cần 1 lần)
docker compose run --rm etl python flows/monthly_pipeline.py simulate
```

Quá trình sẽ:
- Xử lý lần lượt 13 tháng (Dec 2010 → Dec 2011)
- Mỗi tháng: ETL → DWH → RFM snapshot → Gợi ý sản phẩm
- Tổng thời gian: ~5–10 phút

### 5. Xem kết quả

Mở trình duyệt: **http://localhost:8501**

---

## Chạy từng tháng

```bash
# Chạy pipeline cho tháng cụ thể
docker compose run --rm etl python flows/monthly_pipeline.py 2011 6

# Xem logs
docker compose logs -f prefect-worker
```

---

## Xem Data Warehouse (DBeaver)

Kết nối PostgreSQL:
- Host: `localhost`
- Port: `5432`
- Database: `rfm_warehouse`
- User: `rfm_user`
- Password: (giá trị trong .env)

Câu truy vấn hữu ích:

```sql
-- Snapshot tổng hợp
SELECT * FROM vw_segment_monthly ORDER BY period, total_revenue DESC;

-- Migration giữa các kỳ
SELECT * FROM vw_segment_migration ORDER BY period_from, customer_count DESC;

-- Lịch sử một khách hàng
SELECT period, segment, recency, frequency, monetary
FROM rfm_snapshot WHERE customer_id = '12347' ORDER BY period;

-- Kiểm tra pipeline runs
SELECT period, status, rows_loaded, finished_at - started_at AS duration
FROM pipeline_log ORDER BY started_at DESC;
```

---

## Chạy tests

```bash
# Trong Docker
docker compose run --rm etl pytest tests/ -v

# Local (cần Python + requirements.txt)
pip install -r requirements.txt
pytest tests/ -v
```

---

## Prefect UI

Truy cập **http://localhost:4200** để:
- Xem lịch sử chạy pipeline
- Theo dõi trạng thái từng task
- Xem logs chi tiết
- Kích hoạt flow thủ công
- Cấu hình schedule tự động

---

## Trang Lịch sử Phân khúc (Segment History)

Trang mới nhất, hiển thị 4 tab:

| Tab | Nội dung |
|---|---|
| Phân phối theo tháng | Stacked area chart — số KH và doanh thu theo từng phân khúc qua 13 tháng |
| Migration giữa kỳ | Heatmap — khách hàng chuyển từ phân khúc A → B giữa hai tháng bất kỳ |
| Chi tiết phân khúc | Line chart xu hướng + bảng số liệu cho một phân khúc cụ thể |
| Hành trình KH | Theo dõi một khách hàng qua tất cả các kỳ — segment, R/F/M scores |

---

## Dừng và xóa

```bash
# Dừng tất cả
docker compose down

# Dừng và xóa data (reset hoàn toàn)
docker compose down -v
```
