"""dashboard/pages/4_Product_Recommendations.py — Gợi ý Sản phẩm"""

import os, sys
import streamlit as st
import pandas as pd
import plotly.express as px

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config import SEGMENT_COLORS
from dashboard.styles import (GLOBAL_CSS, page_header, section_title, kpi_card,
                               plotly_layout, BORDER, SECONDARY, PRIMARY,
                               TEXT_MUTED, hex_to_rgba)

st.set_page_config(page_title="Gợi ý Sản phẩm", layout="wide")
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

SEGMENT_STRATEGY = {
    "Champions":{"label":"Nâng cấp dịch vụ & Mở rộng danh mục",
     "description":"Khách hàng VIP — ưu tiên trải nghiệm cao cấp, sản phẩm mới và chương trình đại sứ thương hiệu.",
     "actions":[("Email marketing","Thư mời cá nhân hóa: truy cập sớm sản phẩm mới, ưu đãi thành viên VIP."),
                ("Ưu đãi","Bundle cao cấp chiết khấu 5–10%; miễn phí vận chuyển không điều kiện."),
                ("Retargeting","Không cần retargeting tích cực — duy trì quan hệ chất lượng."),
                ("Chương trình","Mời tham gia Đại sứ thương hiệu: referral reward, đánh giá sản phẩm.")]},
    "Loyal Customers":{"label":"Củng cố lòng trung thành & Mở rộng danh mục",
     "description":"Khách hàng mua đều đặn — tập trung tích điểm thành viên và giới thiệu danh mục mới.",
     "actions":[("Email marketing","Bản tin tháng: sản phẩm mới + điểm thưởng sắp hết hạn."),
                ("Ưu đãi","Giảm giá 10–15% danh mục chưa mua; quà tặng dịp đặc biệt."),
                ("Retargeting","Hiển thị sản phẩm liên quan lịch sử mua trên các kênh số."),
                ("Chương trình","Tích điểm đổi quà; nâng hạng dựa trên giá trị tích lũy.")]},
    "Potential Loyalist":{"label":"Nuôi dưỡng & Xây dựng thói quen mua sắm",
     "description":"Tiềm năng cao — cần kích thích tần suất mua để chuyển sang Loyal.",
     "actions":[("Email marketing","Chuỗi 3 email/3 tuần: best-seller, hướng dẫn, đánh giá."),
                ("Ưu đãi","Phiếu giảm giá 15% lần mua thứ 3; gói dùng thử miễn phí."),
                ("Retargeting","Quảng cáo sản phẩm đã xem hoặc giỏ hàng bỏ dở."),
                ("Chương trình","Mời tham gia thành viên; tặng điểm khi hoàn thành hồ sơ.")]},
    "Recent Customers":{"label":"Chào đón & Định hướng mua sắm",
     "description":"Khách hàng mới — hành trình onboarding rõ ràng để xây dựng quan hệ lâu dài.",
     "actions":[("Email marketing","Email chào mừng: 5 sản phẩm bán chạy + hướng dẫn mua lần 2."),
                ("Ưu đãi","Phiếu giảm giá 20% lần mua thứ hai, hết hạn sau 14 ngày."),
                ("Retargeting","Nhắc nhở phiếu giảm giá chưa dùng qua email và quảng cáo."),
                ("Chương trình","Khảo sát trải nghiệm sau mua; tặng điểm khi điền phản hồi.")]},
    "Promising":{"label":"Kích hoạt & Gia tăng giá trị đơn hàng",
     "description":"Tiềm năng nhưng chưa mua nhiều — ưu đãi có thời hạn để thúc đẩy hành động.",
     "actions":[("Email marketing","Flash sale 48 giờ dành riêng nhóm này; nhấn mạnh giới hạn số lượng."),
                ("Ưu đãi","Miễn phí vận chuyển đơn trên £50; bundle tiết kiệm 12%."),
                ("Retargeting","Quảng cáo top sản phẩm; nhắc nhở giỏ hàng bỏ dở."),
                ("Chương trình","Giới thiệu chương trình thành viên; ưu đãi đăng ký lần đầu.")]},
    "Customers Needing Attention":{"label":"Giữ chân & Tái kích hoạt mua sắm",
     "description":"Đang giảm tần suất — cần can thiệp kịp thời để ngăn chặn rời bỏ.",
     "actions":[("Email marketing","Email cá nhân hóa: ưu đãi giới hạn dành riêng bạn."),
                ("Ưu đãi","Phiếu giảm giá 15% sản phẩm từng mua; countdown 72 giờ."),
                ("Retargeting","Ưu tiên kênh mà họ hay tương tác; danh mục quen thuộc."),
                ("Chương trình","Nhắc điểm thưởng sắp hết hạn; tặng điểm bonus khi mua lại.")]},
    "About to Sleep":{"label":"Tái kích hoạt trước khi khách hàng rời bỏ",
     "description":"Nguy cơ ngừng mua — cần chiến dịch nhanh với ưu đãi hấp dẫn.",
     "actions":[("Email marketing","Tiêu đề gây chú ý: 'Lâu rồi không gặp' + sản phẩm nổi bật."),
                ("Ưu đãi","Chiết khấu 20% toàn đơn; quà kèm đơn hàng tái kích hoạt."),
                ("Retargeting","Remarketing mạng xã hội; tần suất cao trong 2 tuần."),
                ("Chương trình","Điểm thưởng sắp mất hiệu lực; ưu đãi đổi điểm ngay.")]},
    "At Risk":{"label":"Chiến dịch Win-back khẩn cấp",
     "description":"Từng có giá trị cao nhưng lâu không mua — can thiệp mạnh để thu hồi.",
     "actions":[("Email marketing","Chuỗi 3 email: Nhắc → Ưu đãi mạnh → Cơ hội cuối. Cách nhau 5 ngày."),
                ("Ưu đãi","Chiết khấu 25–30% sản phẩm từng mua nhiều; hoàn tiền đơn đầu."),
                ("Retargeting","Remarketing đa kênh trong 30 ngày."),
                ("Chương trình","Khảo sát lý do không quay lại; voucher £10 khi hoàn thành.")]},
    "Hibernating":{"label":"Tái tiếp cận thương hiệu",
     "description":"Không hoạt động lâu ngày — nhắc nhở thương hiệu trước khi xóa khỏi danh sách.",
     "actions":[("Email marketing","Email ngắn: 'Chúng tôi có nhiều điều mới' — không ưu đãi sớm."),
                ("Ưu đãi","Ưu đãi theo mùa hoặc dịp lễ; mẫu thử kèm đơn tái kích hoạt."),
                ("Retargeting","Quảng cáo nhận diện thương hiệu; tần suất thấp."),
                ("Chương trình","Sau 2 chiến dịch không phản hồi, xem xét xóa khỏi danh sách gửi thư.")]},
}


@st.cache_data(ttl=120)
def load_periods():
    from etl.db import read_sql
    df = read_sql("SELECT DISTINCT period FROM product_recommendation ORDER BY period DESC")
    return df["period"].tolist()

@st.cache_data(ttl=120)
def load_recommendations(period: str):
    from etl.db import read_sql
    return read_sql("""
        SELECT segment, stock_code, description, score, rank
        FROM product_recommendation
        WHERE period = :p
        ORDER BY segment, rank
    """, params={"p": period})

@st.cache_data(ttl=120)
def load_segment_stats(period: str):
    from etl.db import read_sql
    return read_sql("""
        SELECT segment, COUNT(*) AS customers,
               ROUND(AVG(monetary)::numeric,2) AS avg_monetary,
               ROUND(AVG(frequency)::numeric,2) AS avg_orders
        FROM rfm_snapshot WHERE period = :p GROUP BY segment
    """, params={"p": period})


def render_action_plan(segment: str, color: str):
    strat   = SEGMENT_STRATEGY.get(segment, {})
    actions = strat.get("actions", [])
    rows = "".join(
        f"<tr><td style='font-weight:600;color:{SECONDARY};width:160px;"
        f"padding:0.7rem 1rem;border-bottom:1px solid {BORDER};white-space:nowrap;'>"
        f"{ch}</td>"
        f"<td style='padding:0.7rem 1rem;border-bottom:1px solid {BORDER};"
        f"color:{SECONDARY};line-height:1.5;'>{detail}</td></tr>"
        for ch, detail in actions
    )
    st.markdown(f"""
    <table style="width:100%;border-collapse:collapse;font-size:0.85rem;">
        <thead><tr>
            <th style="background:{BORDER}20;color:{TEXT_MUTED};font-size:0.72rem;
                text-transform:uppercase;letter-spacing:0.05em;padding:0.6rem 1rem;
                text-align:left;border-bottom:1px solid {BORDER};">Kênh</th>
            <th style="background:{BORDER}20;color:{TEXT_MUTED};font-size:0.72rem;
                text-transform:uppercase;letter-spacing:0.05em;padding:0.6rem 1rem;
                text-align:left;border-bottom:1px solid {BORDER};">Nội dung triển khai</th>
        </tr></thead>
        <tbody>{rows}</tbody>
    </table>""", unsafe_allow_html=True)


def main():
    st.markdown(page_header("Gợi ý Sản phẩm theo Phân khúc",
        "Sản phẩm được đề xuất dựa trên hành vi mua sắm của từng phân khúc, kèm kế hoạch marketing."),
        unsafe_allow_html=True)

    try:
        periods = load_periods()
    except Exception as e:
        st.error(f"Lỗi: {e}")
        st.stop()

    if not periods:
        st.warning("Chưa có dữ liệu gợi ý. Chạy simulation pipeline.")
        st.stop()

    # Chọn kỳ + chế độ xem
    col_p, col_m = st.columns([2, 3])
    with col_p:
        period = st.selectbox("Kỳ dữ liệu", periods)
    with col_m:
        view   = st.radio("Chế độ xem", ["Theo phân khúc","So sánh tổng hợp"],
                          horizontal=True)

    reco_df   = load_recommendations(period)
    seg_stats = load_segment_stats(period)

    if reco_df.empty:
        st.warning("Chưa có gợi ý cho kỳ này.")
        st.stop()

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── Chế độ: Theo phân khúc ────────────────────────────────────────────
    if view == "Theo phân khúc":
        segments = sorted(reco_df["segment"].unique())
        selected = st.selectbox("Phân khúc khách hàng", segments)
        seg_data = reco_df[reco_df["segment"] == selected].copy()
        stats_row = seg_stats[seg_stats["segment"] == selected]
        color     = SEGMENT_COLORS.get(selected, PRIMARY)
        strat     = SEGMENT_STRATEGY.get(selected, {})

        # Strategy card
        st.markdown(f"""
        <div style="background:{hex_to_rgba(color,0.06)};border-left:4px solid {color};
                    border-radius:8px;padding:1.25rem 1.5rem;margin:0.75rem 0 1.25rem;">
            <div style="font-size:0.95rem;font-weight:600;color:{SECONDARY};margin-bottom:0.4rem;">
                {strat.get('label','')}</div>
            <div style="font-size:0.85rem;color:{TEXT_MUTED};line-height:1.5;">
                {strat.get('description','')}</div>
        </div>""", unsafe_allow_html=True)

        # KPI
        if not stats_row.empty:
            r = stats_row.iloc[0]
            k1,k2,k3,k4 = st.columns(4)
            for col_k, label, value, sub in [
                (k1,"Khách hàng",           f"{int(r['customers']):,}",   "trong phân khúc"),
                (k2,"Chi tiêu TB/KH",        f"£{r['avg_monetary']:,.1f}", "tổng chi tiêu / số KH"),
                (k3,"Đơn hàng TB/KH",        f"{r['avg_orders']:.1f}",    "đơn duy nhất / số KH"),
                (k4,"Sản phẩm gợi ý",        str(len(seg_data)),           "sản phẩm"),
            ]:
                with col_k:
                    st.markdown(kpi_card(label, value, sub), unsafe_allow_html=True)

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown(section_title(f"Top {len(seg_data)} sản phẩm gợi ý — {selected}"),
                    unsafe_allow_html=True)

        cc, ct = st.columns([3,2])
        with cc:
            seg_data["label"] = seg_data["stock_code"] + "  " + seg_data["description"].str[:28]
            fig = px.bar(seg_data.sort_values("score"),
                x="score", y="label", orientation="h",
                color="score",
                color_continuous_scale=[[0,hex_to_rgba(color,0.15)],[1,color]],
                labels={"score":"Điểm gợi ý (0–100)","label":""},
                text="score", custom_data=["rank"])
            fig.update_traces(texttemplate="%{text:.1f}", textposition="outside",
                hovertemplate="#%{customdata[0]}  <b>%{y}</b><br>Điểm: %{x:.1f}<extra></extra>")
            fig.update_layout(**plotly_layout(coloraxis_showscale=False, height=400,
                title=dict(text="Điểm gợi ý theo sản phẩm", font_size=14)))
            st.plotly_chart(fig, use_container_width=True)
        with ct:
            show = seg_data[["rank","stock_code","description","score"]].copy()
            show["score"] = show["score"].map("{:.1f}".format)
            show.columns = ["#","Mã SP","Tên sản phẩm","Điểm"]
            st.dataframe(show, use_container_width=True, hide_index=True, height=400)

        # Action plan
        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        st.markdown(section_title("Kế hoạch triển khai Marketing"), unsafe_allow_html=True)
        render_action_plan(selected, color)

    # ── Chế độ: So sánh tổng hợp ──────────────────────────────────────────
    else:
        st.markdown(section_title("Top 5 sản phẩm gợi ý theo từng phân khúc"),
                    unsafe_allow_html=True)

        cols = st.columns(3)
        for i, seg in enumerate(sorted(reco_df["segment"].unique())):
            top5  = reco_df[reco_df["segment"]==seg].head(5)
            color = SEGMENT_COLORS.get(seg, PRIMARY)
            items = "".join(
                f"<tr><td style='color:{TEXT_MUTED};font-size:0.75rem;padding:0.3rem 0;width:20px;'>"
                f"{int(r['rank'])}</td>"
                f"<td style='font-size:0.8rem;padding:0.3rem 0;'>"
                f"<span style='font-weight:600;color:{SECONDARY};'>{r['stock_code']}</span> "
                f"{str(r['description'])[:26]}</td>"
                f"<td style='font-size:0.75rem;color:{color};font-weight:600;padding:0.3rem 0;"
                f"text-align:right;'>{r['score']:.0f}</td></tr>"
                for _, r in top5.iterrows()
            )
            with cols[i % 3]:
                st.markdown(f"""
                <div style="border:1px solid {BORDER};border-radius:8px;padding:1rem;
                            margin:0.4rem 0;background:#fff;">
                    <div style="font-size:0.8rem;font-weight:700;color:{color};
                                border-bottom:2px solid {color};padding-bottom:0.4rem;
                                margin-bottom:0.5rem;">{seg}</div>
                    <table style="width:100%;border-collapse:collapse;">{items}</table>
                </div>""", unsafe_allow_html=True)

        st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
        csv = reco_df.to_csv(index=False).encode("utf-8")
        st.download_button("Tải toàn bộ dữ liệu gợi ý sản phẩm (CSV)",
            data=csv, file_name=f"recommendations_{period}.csv", mime="text/csv")

if __name__ == "__main__":
    main()
