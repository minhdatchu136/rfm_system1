"""dashboard/app.py — Trang chủ RFM Analytics Platform v2"""

import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import SEGMENT_COLORS
from dashboard.styles import GLOBAL_CSS, kpi_card, page_header, plotly_layout, BORDER, SECONDARY, TEXT_MUTED

st.set_page_config(page_title="RFM Analytics v2", layout="wide",
                   initial_sidebar_state="expanded")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

@st.cache_data(ttl=120)
def load_summary():
    try:
        from etl.db import read_sql
        customers = read_sql("SELECT COUNT(*) FROM dim_customer").iloc[0,0]
        products  = read_sql("SELECT COUNT(*) FROM dim_product").iloc[0,0]
        orders    = read_sql("SELECT COUNT(DISTINCT invoice_no) FROM fact_sales").iloc[0,0]
        revenue   = read_sql("SELECT COALESCE(SUM(total_amount),0) FROM fact_sales").iloc[0,0]
        rfm_count = read_sql("SELECT COUNT(*) FROM customer_rfm").iloc[0,0]
        periods   = read_sql("SELECT COUNT(DISTINCT period) FROM rfm_snapshot").iloc[0,0]
        segments  = read_sql("""
            SELECT segment, COUNT(*) AS n FROM customer_rfm GROUP BY segment
        """)
        latest_period = read_sql("""
            SELECT MAX(period) FROM rfm_snapshot
        """).iloc[0,0]
        return dict(customers=customers, products=products, orders=orders,
                    revenue=float(revenue), rfm_count=rfm_count,
                    periods=periods, segments=segments,
                    latest_period=latest_period, error=None)
    except Exception as e:
        return dict(error=str(e))

def render_sidebar(data):
    with st.sidebar:
        st.markdown("### Hệ thống")
        st.markdown(f"**Khách hàng:** {data.get('customers','—'):,}" if isinstance(data.get('customers'),int) else "**Khách hàng:** —")
        st.markdown(f"**Snapshots:** {data.get('periods','—')} kỳ")
        st.markdown(f"**Kỳ mới nhất:** {data.get('latest_period','—')}")
        st.markdown("---")
        #st.markdown("**Chạy simulation:**")
        #st.code("docker compose run etl python flows/monthly_pipeline.py simulate", language="bash")

data = load_summary()
render_sidebar(data)

st.markdown(page_header("RFM Analytics Platform",
    "Hệ thống phân tích khách hàng RFM."),
    unsafe_allow_html=True)

if data.get("error"):
    st.error(f"Không thể kết nối PostgreSQL: {data['error']}")
    st.info("Hãy chạy `docker compose up` để khởi động hệ thống.")
    st.stop()

# KPI row — format revenue compactly to avoid line break
def fmt_revenue(v):
    if v >= 1_000_000:
        return f"£{v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"£{v/1_000:.1f}K"
    return f"£{v:,.0f}"

c1,c2,c3,c4,c5,c6 = st.columns(6)
rev_val = data.get('revenue', 0)
cards = [
    (c1, "Khách hàng",   f"{data.get('customers',0):,}",          "",        "#0369A1"),
    (c2, "Sản phẩm",     f"{data.get('products',0):,}",           "",        "#0891B2"),
    (c3, "Đơn hàng",     f"{data.get('orders',0):,}",             "hóa đơn", "#0D9488"),
    (c4, "Doanh thu",    fmt_revenue(rev_val),                    f"£{rev_val:,.0f}", "#DC2626"),
    (c5, "Phân khúc RFM",f"{data.get('rfm_count',0):,}",         "KH hiện tại","#7C3AED"),
    (c6, "Lịch sử",      f"{data.get('periods',0)} kỳ",           "snapshots","#D97706"),
]
for col, label, value, sub, accent in cards:
    with col:
        st.markdown(f"""
        <div style="background:#fff;border:1px solid {BORDER};border-top:3px solid {accent};
                    border-radius:8px;padding:1.1rem 1.25rem;min-height:95px;">
            <div style="font-size:0.7rem;font-weight:600;color:#64748B;text-transform:uppercase;
                        letter-spacing:0.06em;margin-bottom:0.45rem;">{label}</div>
            <div style="font-size:1.6rem;font-weight:700;color:{SECONDARY};
                        letter-spacing:-0.02em;line-height:1;">{value}</div>
            <div style="font-size:0.72rem;color:#94A3B8;margin-top:0.3rem;">{sub}</div>
        </div>""", unsafe_allow_html=True)

st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

# Segment chart
if data.get("rfm_count",0) > 0 and "segments" in data:
    seg_df = data["segments"].sort_values("n", ascending=False)
    total  = seg_df["n"].sum()
    seg_df["pct"] = (seg_df["n"]/total*100).round(1)

    fig = px.bar(seg_df, x="segment", y="n", color="segment",
                 color_discrete_map=SEGMENT_COLORS,
                 text=seg_df["n"].apply(lambda v: f"{v:,}"),
                 custom_data=["pct"],
                 labels={"n":"Số khách hàng","segment":"Phân khúc"})
    fig.update_traces(textposition="outside",
        hovertemplate="<b>%{x}</b><br>Khách hàng: %{y:,}<br>Tỷ lệ: %{customdata[0]:.1f}%<extra></extra>")
    fig.update_layout(**plotly_layout(showlegend=False, height=360,
        xaxis_tickangle=-30, xaxis_title="", yaxis_title="Số khách hàng",
        title=dict(text=f"Phân phối khách hàng hiện tại (kỳ {data.get('latest_period','')})", font_size=14)))
    st.plotly_chart(fig, use_container_width=True)

# Nav cards
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
nav = st.columns(4)
items = [
    ("Tổng quan Kinh doanh", "Doanh thu, xu hướng, top sản phẩm & thị trường"),
    ("Phân tích RFM", "Phân tích 9 phân khúc"),
    ("Lịch sử Phân khúc", "Snapshot 13 tháng, sự chuyển dịch khách hàng giữa các phân khúc"),
    ("Gợi ý Sản phẩm", "Sản phẩm đề xuất + kế hoạch marketing theo phân khúc"),
]
for col, (title, desc) in zip(nav, items):
    with col:
        st.markdown(f"""
        <div style="border:1px solid {BORDER};border-radius:8px;padding:1.25rem;
                    background:#fff;min-height:110px;">
            <div style="font-size:0.9rem;font-weight:600;color:{SECONDARY};margin-bottom:0.4rem;">{title}</div>
            <div style="font-size:0.8rem;color:{TEXT_MUTED};line-height:1.5;">{desc}</div>
        </div>""", unsafe_allow_html=True)
