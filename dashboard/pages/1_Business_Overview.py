"""dashboard/pages/1_Business_Overview.py — Tổng quan Kinh doanh"""

import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from dashboard.styles import (GLOBAL_CSS, page_header, section_title, kpi_card,
                               plotly_layout, BORDER, SECONDARY, PRIMARY,
                               TEXT_MUTED, POSITIVE)

st.set_page_config(page_title="Tổng quan Kinh doanh", layout="wide")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


@st.cache_data(ttl=120)
def load_monthly():
    from etl.db import read_sql
    return read_sql("""
        SELECT
            dd.year, dd.month,
            dd.year::text || '-' || LPAD(dd.month::text,2,'0') AS ym,
            COUNT(DISTINCT f.invoice_no)    AS orders,
            COUNT(DISTINCT f.customer_key)  AS customers,
            ROUND(SUM(f.total_amount)::numeric,2) AS revenue,
            ROUND((SUM(f.total_amount)/NULLIF(COUNT(DISTINCT f.invoice_no),0))::numeric,2) AS aov
        FROM fact_sales f
        JOIN dim_date dd ON f.date_key = dd.date_key
        GROUP BY dd.year, dd.month
        ORDER BY dd.year, dd.month
    """)

@st.cache_data(ttl=120)
def load_top_products():
    from etl.db import read_sql
    return read_sql("""
        SELECT dp.stock_code, dp.description,
               ROUND(SUM(f.total_amount)::numeric,2) AS revenue,
               SUM(f.quantity)                        AS units_sold,
               COUNT(DISTINCT f.invoice_no)           AS orders,
               COUNT(DISTINCT f.customer_key)         AS customers
        FROM fact_sales f
        JOIN dim_product dp ON f.product_key = dp.product_key
        GROUP BY dp.stock_code, dp.description
        ORDER BY revenue DESC LIMIT 15
    """)

@st.cache_data(ttl=120)
def load_by_country():
    from etl.db import read_sql
    return read_sql("""
        SELECT dg.country,
               COUNT(DISTINCT f.customer_key)         AS customers,
               ROUND(SUM(f.total_amount)::numeric,2)  AS revenue,
               COUNT(DISTINCT f.invoice_no)           AS orders
        FROM fact_sales f
        JOIN dim_geography dg ON f.geo_key = dg.geo_key
        GROUP BY dg.country
        ORDER BY revenue DESC
    """)

@st.cache_data(ttl=120)
def load_by_weekday():
    from etl.db import read_sql
    return read_sql("""
        SELECT dd.day_name, dd.day_of_week,
               COUNT(DISTINCT f.invoice_no)          AS orders,
               ROUND(SUM(f.total_amount)::numeric,2) AS revenue
        FROM fact_sales f
        JOIN dim_date dd ON f.date_key = dd.date_key
        GROUP BY dd.day_name, dd.day_of_week
        ORDER BY dd.day_of_week
    """)

@st.cache_data(ttl=120)
def load_cohort():
    from etl.db import read_sql
    return read_sql("""
        SELECT TO_CHAR(first_order,'YYYY-MM') AS cohort_month,
               COUNT(DISTINCT customer_id)    AS new_customers
        FROM dim_customer
        WHERE first_order IS NOT NULL
        GROUP BY cohort_month
        ORDER BY cohort_month
    """)


def main():
    st.markdown(page_header("Tổng quan Kinh doanh",
        "Phân tích hiệu suất theo thời gian, sản phẩm và thị trường."),
        unsafe_allow_html=True)

    try:
        monthly = load_monthly()
    except Exception as e:
        st.error(f"Lỗi kết nối: {e}")
        st.stop()

    # Bộ lọc năm
    years     = sorted(monthly["year"].unique().tolist())
    sel_years = st.multiselect("Lọc theo năm", years, default=years)
    mf        = monthly[monthly["year"].isin(sel_years)]

    if mf.empty:
        st.warning("Không có dữ liệu.")
        st.stop()

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # KPI
    c1,c2,c3,c4 = st.columns(4)
    for col, label, value, sub in [
        (c1,"Doanh thu",              f"£{mf['revenue'].sum():,.0f}",    ""),
        (c2,"Đơn hàng",               f"{mf['orders'].sum():,}",         ""),
        (c3,"Khách hàng hoạt động",   f"{mf['customers'].sum():,}",      ""),
        (c4,"Giá trị trung bình/đơn", f"£{mf['aov'].mean():,.2f}",       "trung bình trong kỳ"),
    ]:
        with col:
            st.markdown(kpi_card(label, value, sub), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Revenue trend
    fig_rev = go.Figure()
    fig_rev.add_trace(go.Bar(x=mf["ym"], y=mf["revenue"],
        name="Doanh thu (£)", marker_color=PRIMARY, opacity=0.85,
        hovertemplate="Tháng %{x}<br>Doanh thu: £%{y:,.0f}<extra></extra>"))
    fig_rev.add_trace(go.Scatter(x=mf["ym"], y=mf["orders"],
        name="Đơn hàng", yaxis="y2", mode="lines+markers",
        line=dict(color=SECONDARY, width=2), marker=dict(size=5),
        hovertemplate="Tháng %{x}<br>Đơn hàng: %{y:,}<extra></extra>"))
    fig_rev.update_layout(**plotly_layout(
        yaxis=dict(title="Doanh thu (£)", gridcolor=BORDER),
        yaxis2=dict(title="Đơn hàng", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.08, x=0),
        height=360, xaxis_tickangle=-45, xaxis_title="",
        title=dict(text="Doanh thu và đơn hàng theo tháng", font_size=14)))
    st.plotly_chart(fig_rev, use_container_width=True)
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # Top products + country
    top_products = load_top_products()
    by_country   = load_by_country()
    cp, cg = st.columns(2)
    with cp:
        top10 = top_products.head(10).copy()
        top10["label"] = top10["stock_code"] + "  " + top10["description"].str[:24]
        fig_p = px.bar(top10.sort_values("revenue"), x="revenue", y="label",
            orientation="h", color="revenue",
            color_continuous_scale=[[0,"#DBEAFE"],[1,PRIMARY]],
            labels={"revenue":"Doanh thu (£)","label":""},
            title="Top 10 sản phẩm theo doanh thu")
        fig_p.update_layout(**plotly_layout(coloraxis_showscale=False, height=380))
        st.plotly_chart(fig_p, use_container_width=True)
    with cg:
        top_c = by_country.head(10).copy()
        fig_g = px.bar(top_c.sort_values("revenue"), x="revenue", y="country",
            orientation="h", color="revenue",
            color_continuous_scale=[[0,"#E0F2FE"],[1,"#0369A1"]],
            labels={"revenue":"Doanh thu (£)","country":""},
            title="Top 10 thị trường theo doanh thu")
        fig_g.update_layout(**plotly_layout(coloraxis_showscale=False, height=380))
        st.plotly_chart(fig_g, use_container_width=True)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # Weekday + cohort
    by_weekday = load_by_weekday()
    cohort     = load_cohort()
    cd, cc = st.columns(2)
    with cd:
        fig_d = px.bar(by_weekday, x="day_name", y="orders",
            color="revenue", color_continuous_scale=[[0,"#FEF3C7"],[1,"#D97706"]],
            labels={"orders":"Đơn hàng","day_name":""},
            title="Đơn hàng theo ngày trong tuần")
        fig_d.update_layout(**plotly_layout(coloraxis_showscale=False, height=300))
        st.plotly_chart(fig_d, use_container_width=True)
    with cc:
        fig_c = px.area(cohort, x="cohort_month", y="new_customers",
            color_discrete_sequence=[POSITIVE],
            labels={"new_customers":"Khách hàng mới","cohort_month":"Tháng"},
            title="Khách hàng mới theo tháng")
        fig_c.update_layout(**plotly_layout(height=300, xaxis_tickangle=-45))
        st.plotly_chart(fig_c, use_container_width=True)

    with st.expander("Xem dữ liệu chi tiết theo tháng"):
        display = mf.rename(columns={"ym":"Kỳ","orders":"Đơn hàng",
            "customers":"Khách hàng","revenue":"Doanh thu (£)","aov":"GTB/Đơn (£)"})
        st.dataframe(display[["Kỳ","Đơn hàng","Khách hàng","Doanh thu (£)","GTB/Đơn (£)"]]
            .style.format({"Doanh thu (£)":"£{:,.2f}","GTB/Đơn (£)":"£{:,.2f}",
                           "Đơn hàng":"{:,}","Khách hàng":"{:,}"}),
            use_container_width=True)

if __name__ == "__main__":
    main()
