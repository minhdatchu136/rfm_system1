"""dashboard/pages/2_RFM_Analysis.py — Phân tích Phân khúc RFM"""

import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import SEGMENT_COLORS
from dashboard.styles import (GLOBAL_CSS, page_header, section_title, kpi_card,
                               plotly_layout, BORDER, SECONDARY, PRIMARY, TEXT_MUTED)

st.set_page_config(page_title="Phân tích RFM", layout="wide")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


@st.cache_data(ttl=120)
def load_rfm():
    from etl.db import read_sql
    return read_sql("SELECT * FROM customer_rfm")

@st.cache_data(ttl=120)
def load_segment_detail(segment: str):
    from etl.db import read_sql
    return read_sql("""
        SELECT cr.customer_id, cr.recency, cr.frequency, cr.monetary,
               cr.rfm_score, dc.country
        FROM customer_rfm cr
        LEFT JOIN dim_customer dc ON cr.customer_id = dc.customer_id
        WHERE cr.segment = :seg
        ORDER BY cr.monetary DESC
    """, params={"seg": segment})


def build_summary(rfm: pd.DataFrame) -> pd.DataFrame:
    return (rfm.groupby("segment")
               .agg(customers=("customer_id","count"),
                    avg_recency=("recency","mean"),
                    avg_frequency=("frequency","mean"),
                    avg_monetary=("monetary","mean"),
                    total_revenue=("monetary","sum"))
               .round(1).reset_index()
               .sort_values("total_revenue", ascending=False))


def main():
    st.markdown(page_header("Phân tích Phân khúc Khách hàng",
        "Phân loại khách hàng thành 9 nhóm dựa trên điểm Recency, Frequency, Monetary."),
        unsafe_allow_html=True)

    try:
        rfm = load_rfm()
    except Exception as e:
        st.error(f"Lỗi: {e}")
        st.stop()

    if rfm.empty:
        st.warning("Chưa có dữ liệu RFM. Chạy simulation pipeline.")
        st.stop()

    summary = build_summary(rfm)

    # Pie + Treemap
    c1, c2 = st.columns(2)
    with c1:
        fig_pie = px.pie(summary, values="customers", names="segment",
            color="segment", color_discrete_map=SEGMENT_COLORS, hole=0.4,
            title="Tỷ lệ khách hàng theo phân khúc")
        fig_pie.update_traces(textposition="inside", textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>KH: %{value:,}<br>%{percent}<extra></extra>")
        fig_pie.update_layout(**plotly_layout(showlegend=False, height=360))
        st.plotly_chart(fig_pie, use_container_width=True)
    with c2:
        fig_tree = px.treemap(summary, path=["segment"], values="total_revenue",
            color="segment", color_discrete_map=SEGMENT_COLORS,
            title="Tỷ trọng doanh thu theo phân khúc")
        fig_tree.update_traces(
            hovertemplate="<b>%{label}</b><br>Doanh thu: £%{value:,.0f}<extra></extra>")
        fig_tree.update_layout(**plotly_layout(height=360))
        st.plotly_chart(fig_tree, use_container_width=True)

    # Summary table
    st.markdown(section_title("Thống kê tổng hợp"), unsafe_allow_html=True)
    display = summary.copy()
    display.columns = ["Phân khúc","Khách hàng","Recency TB (ngày)",
                        "Frequency TB","Monetary TB (£)","Doanh thu (£)"]
    st.dataframe(display.style.format({
        "Monetary TB (£)":"£{:,.1f}","Doanh thu (£)":"£{:,.0f}",
        "Recency TB (ngày)":"{:.0f}","Frequency TB":"{:.1f}","Khách hàng":"{:,}"}),
        use_container_width=True, hide_index=True)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # Boxplot + Avg scores
    cb, cs = st.columns(2)
    with cb:
        fig_box = px.box(rfm, x="segment", y="monetary", color="segment",
            color_discrete_map=SEGMENT_COLORS,
            labels={"monetary":"Monetary (£)","segment":""},
            title="Phân phối Monetary theo phân khúc")
        fig_box.update_layout(**plotly_layout(showlegend=False, height=360,
            xaxis_tickangle=-30))
        st.plotly_chart(fig_box, use_container_width=True)
    with cs:
        score_df = rfm.groupby("segment")[["r_score","f_score","m_score"]]\
                      .mean().round(2).reset_index()
        melted = score_df.melt(id_vars="segment",
            value_vars=["r_score","f_score","m_score"],
            var_name="chi_so", value_name="diem")
        melted["chi_so"] = melted["chi_so"].map(
            {"r_score":"Recency","f_score":"Frequency","m_score":"Monetary"})
        fig_s = px.bar(melted, x="segment", y="diem", color="chi_so",
            barmode="group",
            color_discrete_sequence=[PRIMARY,"#0891B2","#0D9488"],
            labels={"diem":"Điểm TB (1–5)","segment":"","chi_so":"Chỉ số"},
            title="Điểm R/F/M trung bình theo phân khúc")
        fig_s.update_layout(**plotly_layout(height=360, xaxis_tickangle=-30,
            legend=dict(title="")))
        st.plotly_chart(fig_s, use_container_width=True)

    # Drill-down
    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
    st.markdown(section_title("Chi tiết theo phân khúc"), unsafe_allow_html=True)
    seg_list = sorted(rfm["segment"].unique())
    selected = st.selectbox("Chọn phân khúc", seg_list, label_visibility="collapsed")
    detail   = load_segment_detail(selected)
    color    = SEGMENT_COLORS.get(selected, PRIMARY)

    st.markdown(f'<span class="segment-badge" style="background:{color};">'
                f'{selected}</span><br><br>', unsafe_allow_html=True)

    if not detail.empty:
        k1,k2,k3,k4 = st.columns(4)
        for col, label, value, sub in [
            (k1,"Khách hàng",             f"{len(detail):,}",                       ""),
            (k2,"Recency trung bình",      f"{detail['recency'].mean():.0f} ngày",   "kể từ lần mua cuối"),
            (k3,"Frequency trung bình",    f"{detail['frequency'].mean():.1f} đơn",  "trên mỗi KH"),
            (k4,"Monetary trung bình",     f"£{detail['monetary'].mean():,.1f}",      "trên mỗi KH"),
        ]:
            with col:
                st.markdown(kpi_card(label, value, sub), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        ct, ch = st.columns([3,2])
        with ct:
            show = detail[["customer_id","recency","frequency","monetary",
                           "rfm_score","country"]].head(50)
            show.columns = ["Khách hàng","Recency","Frequency","Monetary (£)","Điểm RFM","Quốc gia"]
            st.dataframe(show.style.format({"Monetary (£)":"£{:,.2f}"}),
                         use_container_width=True, hide_index=True, height=340)
        with ch:
            p95 = float(detail["monetary"].astype(float).quantile(0.95))
            has_outlier = float(detail["monetary"].max()) > p95 * 1.5
            fig_h = px.histogram(detail, x="monetary", nbins=30,
                color_discrete_sequence=[color],
                labels={"monetary":"Monetary (£)","count":"Số KH"},
                title="Phân phối Monetary")
            fig_h.update_xaxes(range=[0, p95 * 1.1])
            fig_h.update_layout(**plotly_layout(height=340))
            st.plotly_chart(fig_h, use_container_width=True)
            if has_outlier:
                n_out = int((detail["monetary"] > p95).sum())
                st.caption(f"Hiển thị đến 95th percentile (£{p95:,.0f}). "
                           f"Ẩn {n_out} KH outlier (tối đa £{detail['monetary'].max():,.0f}).")

if __name__ == "__main__":
    main()
