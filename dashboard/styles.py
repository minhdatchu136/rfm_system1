"""dashboard/styles.py — CSS và helpers dùng chung"""

PRIMARY    = "#2563EB"
SECONDARY  = "#0F172A"
SURFACE    = "#F8FAFC"
BORDER     = "#E2E8F0"
TEXT_MUTED = "#64748B"
POSITIVE   = "#16A34A"
NEGATIVE   = "#DC2626"

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

GLOBAL_CSS = f"""
<style>
  html, body, [class*="css"] {{
    font-family: 'Inter', -apple-system, sans-serif;
  }}
  #MainMenu, footer, header {{ visibility: hidden; }}
  .block-container {{ padding: 2rem 2.5rem 3rem; max-width: 1280px; }}
  .page-header {{ border-bottom: 1px solid {BORDER}; padding-bottom: 1.25rem; margin-bottom: 1.75rem; }}
  .page-title {{ font-size: 1.75rem; font-weight: 700; color: {SECONDARY}; margin: 0 0 0.25rem; letter-spacing: -0.02em; }}
  .page-subtitle {{ font-size: 0.875rem; color: {TEXT_MUTED}; margin: 0; }}
  .kpi-card {{ background: #FFFFFF; border: 1px solid {BORDER}; border-radius: 10px; padding: 1.25rem 1.5rem; min-height: 100px; }}
  .kpi-label {{ font-size: 0.75rem; font-weight: 600; color: {TEXT_MUTED}; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.5rem; }}
  .kpi-value {{ font-size: 1.875rem; font-weight: 700; color: {SECONDARY}; letter-spacing: -0.03em; line-height: 1; }}
  .kpi-sub   {{ font-size: 0.75rem; color: {TEXT_MUTED}; margin-top: 0.35rem; }}
  .section-title {{ font-size: 1rem; font-weight: 600; color: {SECONDARY}; margin: 0 0 1rem; padding-bottom: 0.5rem; border-bottom: 2px solid {PRIMARY}; display: inline-block; }}
  .section-divider {{ border: none; border-top: 1px solid {BORDER}; margin: 1.75rem 0; }}
  .status-ok   {{ color: {POSITIVE}; font-weight: 600; font-size: 0.85rem; }}
  .status-warn {{ color: #D97706; font-weight: 600; font-size: 0.85rem; }}
  .status-err  {{ color: {NEGATIVE}; font-weight: 600; font-size: 0.85rem; }}
  .segment-badge {{ display: inline-block; padding: 0.2rem 0.65rem; border-radius: 20px; font-size: 0.75rem; font-weight: 600; color: white; }}
</style>
"""

PLOT_LAYOUT = dict(
    plot_bgcolor  = "rgba(0,0,0,0)",
    paper_bgcolor = "rgba(0,0,0,0)",
    font          = dict(family="Inter, sans-serif", color=SECONDARY, size=12),
    margin        = dict(l=0, r=0, t=40, b=0),
    hoverlabel    = dict(bgcolor="white", font_size=12, bordercolor=BORDER),
)

def plotly_layout(**kwargs) -> dict:
    d = PLOT_LAYOUT.copy()
    d.update(kwargs)
    return d

def kpi_card(label, value, sub=""):
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f"""<div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {sub_html}</div>"""

def page_header(title, subtitle=""):
    sub = f'<p class="page-subtitle">{subtitle}</p>' if subtitle else ""
    return f"""<div class="page-header">
        <h1 class="page-title">{title}</h1>{sub}</div>"""

def section_title(text):
    return f'<p class="section-title">{text}</p>'

def hex_to_rgba(hex_color: str, alpha: float = 0.15) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return f"rgba({r},{g},{b},{alpha})"
