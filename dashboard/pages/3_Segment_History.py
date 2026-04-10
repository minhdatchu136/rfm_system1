"""
dashboard/pages/3_Segment_History.py
Lịch sử phân khúc RFM theo 13 tháng + migration matrix
"""
import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import SEGMENT_COLORS
from dashboard.styles import (GLOBAL_CSS, page_header, section_title,
                               kpi_card, plotly_layout, BORDER, SECONDARY, TEXT_MUTED)

st.set_page_config(page_title="Lịch sử Phân khúc", layout="wide")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# Data loaders
# ══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=120)
def load_snapshot_summary():
    from etl.db import read_sql
    return read_sql("""
        SELECT period, segment,
               COUNT(*)                           AS customer_count,
               ROUND(AVG(recency)::numeric,1)     AS avg_recency,
               ROUND(AVG(frequency)::numeric,1)   AS avg_frequency,
               ROUND(AVG(monetary)::numeric,2)    AS avg_monetary,
               ROUND(SUM(monetary)::numeric,2)    AS total_revenue
        FROM rfm_snapshot
        GROUP BY period, segment
        ORDER BY period, total_revenue DESC
    """)

@st.cache_data(ttl=120)
def load_period_list():
    from etl.db import read_sql
    df = read_sql("SELECT DISTINCT period FROM rfm_snapshot ORDER BY period")
    return df["period"].tolist()

@st.cache_data(ttl=120)
def load_segment_timeseries():
    from etl.db import read_sql
    return read_sql("""
        SELECT period, segment, COUNT(*) AS n, ROUND(SUM(monetary)::numeric,2) AS revenue
        FROM rfm_snapshot
        GROUP BY period, segment
        ORDER BY period, segment
    """)

@st.cache_data(ttl=120)
def load_migration_matrix(period_from: str, period_to: str):
    from etl.db import read_sql
    return read_sql("""
        SELECT a.segment AS from_seg, b.segment AS to_seg, COUNT(*) AS customers
        FROM rfm_snapshot a
        JOIN rfm_snapshot b ON a.customer_id = b.customer_id
        WHERE a.period = :pf AND b.period = :pt
        GROUP BY a.segment, b.segment
        ORDER BY customers DESC
    """, params={"pf": period_from, "pt": period_to})

@st.cache_data(ttl=120)
def load_customer_journey(customer_id: str):
    from etl.db import read_sql
    return read_sql("""
        SELECT period, segment, recency, frequency, monetary,
               r_score, f_score, m_score
        FROM rfm_snapshot
        WHERE customer_id = :cid
        ORDER BY period
    """, params={"cid": customer_id})

# ══════════════════════════════════════════════════════════════════════════
# Chart builders
# ══════════════════════════════════════════════════════════════════════════

def chart_migration_heatmap(mig_df: pd.DataFrame,
                             period_from: str, period_to: str) -> go.Figure:
    """Heatmap migration giữa hai tháng."""
    segs = list(SEGMENT_COLORS.keys())
    pivot = mig_df.pivot_table(index="from_seg", columns="to_seg",
                                values="customers", fill_value=0)
    # Đảm bảo đủ hàng/cột
    pivot = pivot.reindex(index=segs, columns=segs, fill_value=0)

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=segs, y=segs,
        colorscale="Blues",
        text=pivot.values,
        texttemplate="%{text}",
        hovertemplate="Từ: <b>%{y}</b><br>Đến: <b>%{x}</b><br>KH: %{z}<extra></extra>",
    ))
    fig.update_layout(**plotly_layout(
        height=500,
        xaxis_tickangle=-35,
        title=dict(text=f"Migration: {period_from} → {period_to}", font_size=14),
        xaxis_title="Phân khúc đến (kỳ sau)",
        yaxis_title="Phân khúc từ (kỳ trước)",
    ))
    return fig


def chart_segment_trend_line(summary: pd.DataFrame,
                              segment: str) -> go.Figure:
    """Line chart 3 chỉ số cho một phân khúc qua thời gian."""
    df = summary[summary["segment"] == segment].sort_values("period")
    fig = go.Figure()
    metrics = [
        ("customer_count", "Số KH",      "#2563EB"),
        ("avg_monetary",   "Avg £ (KH)", "#0891B2"),
        ("avg_recency",    "Avg Recency","#EA580C"),
    ]
    for col, name, color in metrics:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df["period"], y=df[col], name=name,
                mode="lines+markers",
                line=dict(color=color, width=2),
                marker=dict(size=6),
                hovertemplate=f"<b>{name}</b><br>%{{x}}: %{{y:,.1f}}<extra></extra>",
            ))
    fig.update_layout(**plotly_layout(
        height=320, xaxis_tickangle=-30,
        title=dict(text=f"Xu hướng — {segment}", font_size=13),
        legend=dict(orientation="h", y=1.1),
    ))
    return fig

# ══════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════

def main():
    st.markdown(page_header(
        "Lịch sử Phân khúc Khách hàng",
        "Snapshot RFM theo 13 tháng — theo dõi biến động và sự chuyển dịch giữa các phân khúc."),
        unsafe_allow_html=True)

    try:
        periods  = load_period_list()
        summary  = load_snapshot_summary()
        ts_df    = load_segment_timeseries()
    except Exception as e:
        st.error(f"Không thể tải dữ liệu: {e}")
        st.info("Hãy chạy simulation trước: `python flows/monthly_pipeline.py simulate`")
        st.stop()

    if not periods:
        st.warning("Chưa có dữ liệu snapshot. Chạy simulation pipeline.")
        st.stop()

    # ── KPI tổng quan ─────────────────────────────────────────────────────
    total_customers = summary.groupby("period")["customer_count"].sum()
    first_p, last_p = periods[0], periods[-1]

    c1,c2,c3,c4 = st.columns(4)
    for col, label, value, sub in [
        (c1, "Số kỳ dữ liệu",   f"{len(periods)} tháng",              f"{first_p} → {last_p}"),
        (c2, "KH kỳ đầu",       f"{total_customers[first_p]:,}",       first_p),
        (c3, "KH kỳ cuối",      f"{total_customers[last_p]:,}",        last_p),
        (c4, "Tăng trưởng KH",  f"+{total_customers[last_p]-total_customers[first_p]:,}", "kỳ đầu → kỳ cuối"),
    ]:
        with col:
            st.markdown(kpi_card(label, value, sub), unsafe_allow_html=True)

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── Tab layout ────────────────────────────────────────────────────────
    tab2, tab3, tab4 = st.tabs([
        "Sự chuyển dịch giữa kỳ",
        "Chi tiết phân khúc",
        "Hành trình khách hàng",
    ])

    # ── Tab 1: Migration heatmap ───────────────────────────────────────────
    with tab2:
        st.markdown(section_title("Sự chuyển dịch khách hàng giữa hai kỳ"),
                    unsafe_allow_html=True)
        st.caption("Xem có bao nhiêu khách từ phân khúc A (kỳ trước) chuyển sang phân khúc B (kỳ sau).")

        col_f, col_t = st.columns(2)
        with col_f:
            period_from = st.selectbox("Kỳ trước", periods[:-1], index=len(periods)-2)
        with col_t:
            available_to = [p for p in periods if p > period_from]
            period_to   = st.selectbox("Kỳ sau",  available_to, index=0)

        mig_df = load_migration_matrix(period_from, period_to)

        if mig_df.empty:
            st.info("Không có dữ liệu chuyển dịch giữa hai kỳ này.")
        else:
            st.plotly_chart(chart_migration_heatmap(mig_df, period_from, period_to),
                            use_container_width=True)

            # Top migrations
            st.markdown(section_title("Top thay đổi phân khúc đáng chú ý"),
                        unsafe_allow_html=True)
            top_changes = mig_df[mig_df["from_seg"] != mig_df["to_seg"]]\
                              .sort_values("customers", ascending=False).head(10)
            if not top_changes.empty:
                display = top_changes.copy()
                display.columns = ["Phân khúc cũ", "Phân khúc mới", "Số KH chuyển"]
                st.dataframe(display, use_container_width=True, hide_index=True)

    # ── Tab 3: Chi tiết từng phân khúc ────────────────────────────────────
    with tab3:
        all_segs = sorted(ts_df["segment"].unique())
        sel_seg  = st.selectbox("Chọn phân khúc", all_segs)
        seg_color = SEGMENT_COLORS.get(sel_seg, "#2563EB")

        st.markdown(
            f'<span class="segment-badge" style="background:{seg_color};">'
            f'{sel_seg}</span>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        st.plotly_chart(chart_segment_trend_line(summary, sel_seg),
                        use_container_width=True)

        # Bảng chi tiết
        seg_detail = summary[summary["segment"] == sel_seg].sort_values("period")
        seg_detail_show = seg_detail.rename(columns={
            "period": "Kỳ", "customer_count": "Số KH",
            "avg_recency": "Avg Recency (ngày)", "avg_frequency": "Avg Frequency",
            "avg_monetary": "Avg Monetary (£)", "total_revenue": "Doanh thu (£)",
        })
        st.dataframe(
            seg_detail_show.style.format({
                "Avg Monetary (£)": "£{:,.1f}",
                "Doanh thu (£)"   : "£{:,.0f}",
            }),
            use_container_width=True, hide_index=True,
        )

    # ── Tab 4: Hành trình khách hàng ──────────────────────────────────────
    with tab4:
        st.markdown(section_title("Theo dõi hành trình một khách hàng"),
                    unsafe_allow_html=True)

        # Lấy sample customer IDs
        try:
            from etl.db import read_sql
            sample_custs = read_sql("""
                SELECT customer_id, COUNT(DISTINCT period) AS n_periods
                FROM rfm_snapshot
                GROUP BY customer_id
                HAVING COUNT(DISTINCT period) >= 6
                ORDER BY n_periods DESC
                LIMIT 100
            """)
            cust_options = sample_custs["customer_id"].tolist()
        except Exception:
            cust_options = []

        if not cust_options:
            st.info("Chưa có dữ liệu hành trình.")
        else:
            col_cust, col_info = st.columns([1, 2])
            with col_cust:
                sel_cust = st.selectbox(
                    "Chọn khách hàng (có ≥ 6 kỳ dữ liệu)",
                    cust_options,
                    format_func=lambda x: f"KH #{x}",
                )

            journey = load_customer_journey(sel_cust)
            if journey.empty:
                st.info("Không tìm thấy dữ liệu.")
            else:
                with col_info:
                    st.metric("Số kỳ xuất hiện", f"{len(journey)}")
                    st.metric("Phân khúc hiện tại", journey.iloc[-1]["segment"])
                    st.metric("Monetary kỳ cuối", f"£{journey.iloc[-1]['monetary']:,.0f}")

                # Timeline chart
                colors_journey = [SEGMENT_COLORS.get(s,"#888") for s in journey["segment"]]
                fig_j = go.Figure()
                fig_j.add_trace(go.Scatter(
                    x=journey["period"], y=journey["rfm_total"] if "rfm_total" in journey.columns else journey["monetary"]/100,
                    mode="lines+markers+text",
                    text=journey["segment"].str.split().str[0],
                    textposition="top center",
                    marker=dict(color=colors_journey, size=12, symbol="circle"),
                    line=dict(color="#94A3B8", width=1.5),
                    hovertemplate=(
                        "<b>Kỳ: %{x}</b><br>"
                        "Segment: " + journey["segment"].astype(str) + "<br>"
                        "R=%{customdata[0]} F=%{customdata[1]} M=%{customdata[2]}<extra></extra>"
                    ),
                    customdata=journey[["r_score","f_score","m_score"]].values,
                ))
                fig_j.update_layout(**plotly_layout(
                    height=300, showlegend=False,
                    title=dict(text=f"Hành trình phân khúc — KH #{sel_cust}", font_size=13),
                    xaxis_title="", yaxis_title="Điểm tổng hợp",
                ))
                st.plotly_chart(fig_j, use_container_width=True)

                # Bảng chi tiết
                show_journey = journey[["period","segment","recency","frequency",
                                        "monetary","r_score","f_score","m_score"]]
                show_journey.columns = ["Kỳ","Phân khúc","Recency","Frequency",
                                        "Monetary (£)","R","F","M"]
                st.dataframe(
                    show_journey.style.format({"Monetary (£)": "£{:,.0f}"}),
                    use_container_width=True, hide_index=True,
                )


if __name__ == "__main__":
    main()
