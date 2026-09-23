"""
E-Commerce Sales & Revenue Intelligence Dashboard
A modern, production-grade retail analytics platform for business executives and sales analysts.
"""

import sys
from pathlib import Path
import html

# Ensure project root is available in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from src.app.data_service import DataService

# -----------------------------------------------------------------------------
# 1. Page Configuration
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="E-Commerce Sales & Revenue Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------------------------------
# 2. Theme State Management
# -----------------------------------------------------------------------------
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

def toggle_theme():
    st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"

IS_DARK = st.session_state.theme == "dark"

# -----------------------------------------------------------------------------
# 3. CSS Design System (Zinc Dark / Light Minimalist Palette)
# -----------------------------------------------------------------------------
CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,100..1000;1,9..40,100..1000&family=JetBrains+Mono:wght@400;500;600&display=swap');

:root {{
    --bg: {"#09090b" if IS_DARK else "#ffffff"};
    --bg-subtle: {"#0f0f13" if IS_DARK else "#f8fafc"};
    --card: {"#121216" if IS_DARK else "#ffffff"};
    --card-hover: {"#181820" if IS_DARK else "#f1f5f9"};
    --border: {"#24242c" if IS_DARK else "#e2e8f0"};
    --border-subtle: {"#1b1b22" if IS_DARK else "#f1f5f9"};
    --text: {"#f4f4f5" if IS_DARK else "#09090b"};
    --text-muted: {"#a1a1aa" if IS_DARK else "#64748b"};
    --text-dim: {"#71717a" if IS_DARK else "#94a3b8"};
    --accent: #2563eb;
    --accent-muted: rgba(37, 99, 235, 0.15);
    --green: {"#22c55e" if IS_DARK else "#16a34a"};
    --green-muted: {"rgba(34, 197, 94, 0.15)" if IS_DARK else "rgba(22, 163, 74, 0.12)"};
    --red: {"#ef4444" if IS_DARK else "#dc2626"};
    --red-muted: {"rgba(239, 68, 68, 0.15)" if IS_DARK else "rgba(220, 38, 38, 0.12)"};
    --amber: {"#f59e0b" if IS_DARK else "#d97706"};
    --amber-muted: {"rgba(245, 158, 11, 0.15)" if IS_DARK else "rgba(217, 119, 6, 0.12)"};
    --purple: #a855f7;
    --purple-muted: rgba(168, 85, 247, 0.15);
    --shadow: {"0 4px 20px rgba(0, 0, 0, 0.35)" if IS_DARK else "0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04)"};
    --radius: 12px;
}}

/* Streamlit Chrome Reset */
header[data-testid="stHeader"], #MainMenu, footer, [data-testid="stToolbar"],
[data-testid="stDecoration"], [data-testid="stStatusWidget"], .stDeployButton,
div[data-testid="stSidebarCollapsedControl"] {{
    display: none !important;
}}

html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], .main, .block-container, section[data-testid="stMain"] {{
    background-color: var(--bg) !important;
    color: var(--text) !important;
    font-family: 'DM Sans', -apple-system, sans-serif !important;
}}

.block-container {{
    padding: 1.5rem 2.5rem 3.5rem !important;
    max-width: 1440px !important;
}}

code, pre, .mono {{
    font-family: 'JetBrains Mono', monospace !important;
}}

/* Pill-Style Tabs */
[data-baseweb="tab-list"] {{
    gap: 6px !important;
    background: var(--bg-subtle) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    padding: 4px !important;
    margin-bottom: 1.5rem !important;
}}

button[data-baseweb="tab"] {{
    background: transparent !important;
    color: var(--text-muted) !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    padding: 0.6rem 1.2rem !important;
    border: 1px solid transparent !important;
    border-radius: 8px !important;
    transition: all 0.15s ease-in-out !important;
}}

button[data-baseweb="tab"]:hover {{
    color: var(--text) !important;
    background: var(--card-hover) !important;
}}

button[data-baseweb="tab"][aria-selected="true"] {{
    color: var(--text) !important;
    background: var(--card) !important;
    border-color: var(--border) !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.12) !important;
}}

[data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {{
    display: none !important;
}}

/* Top Brand & Filter Bar */
.brand-container {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding-bottom: 1.25rem;
    margin-bottom: 1.25rem;
    border-bottom: 1px solid var(--border);
}}

.brand-title {{
    font-size: 1.45rem;
    font-weight: 700;
    color: var(--text);
    display: flex;
    align-items: center;
    gap: 10px;
    letter-spacing: -0.02em;
}}

.brand-badge {{
    font-size: 0.72rem;
    font-weight: 600;
    padding: 3px 8px;
    border-radius: 6px;
    background: var(--green-muted);
    color: var(--green);
    border: 1px solid rgba(34, 197, 94, 0.3);
}}

.brand-desc {{
    font-size: 0.84rem;
    color: var(--text-muted);
    margin-top: 3px;
}}

/* Filter Bar Box */
.filter-bar {{
    background: var(--bg-subtle);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.85rem 1.25rem;
    margin-bottom: 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
}}

/* Metric Cards */
.metric-card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.2rem 1.35rem;
    box-shadow: var(--shadow);
    transition: transform 0.15s ease, border-color 0.15s ease;
}}
.metric-card:hover {{
    border-color: var(--accent);
    transform: translateY(-2px);
}}

.metric-label {{
    font-size: 0.76rem;
    color: var(--text-muted);
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 0.35rem;
}}

.metric-value {{
    font-size: 1.85rem;
    font-weight: 700;
    color: var(--text);
    letter-spacing: -0.03em;
    line-height: 1.15;
}}

.metric-delta {{
    font-size: 0.75rem;
    font-weight: 500;
    margin-top: 0.45rem;
    padding: 2px 8px;
    border-radius: 6px;
    display: inline-flex;
    align-items: center;
    gap: 4px;
}}

.delta-up {{ color: var(--green); background: var(--green-muted); }}
.delta-down {{ color: var(--red); background: var(--red-muted); }}
.delta-warn {{ color: var(--amber); background: var(--amber-muted); }}
.delta-info {{ color: var(--accent); background: var(--accent-muted); }}

/* Chart Containers */
.chart-wrap {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.25rem 1.25rem 0.75rem;
    box-shadow: var(--shadow);
    margin-bottom: 1.25rem;
}}

.chart-header {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    margin-bottom: 0.75rem;
}}

.chart-title {{
    font-size: 0.92rem;
    font-weight: 600;
    color: var(--text);
}}

.chart-subtitle {{
    font-size: 0.75rem;
    color: var(--text-dim);
}}

/* Custom HTML Tables */
.data-table-wrap {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    overflow-x: auto;
    box-shadow: var(--shadow);
    margin-bottom: 1.25rem;
}}

.data-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 0.83rem;
}}

.data-table th {{
    text-align: left;
    padding: 0.7rem 0.95rem;
    color: var(--text-muted);
    font-weight: 600;
    font-size: 0.73rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    background: var(--bg-subtle);
    border-bottom: 1px solid var(--border);
}}

.data-table td {{
    padding: 0.68rem 0.95rem;
    color: var(--text);
    border-bottom: 1px solid var(--border-subtle);
}}

.data-table tr:hover td {{
    background: var(--card-hover);
}}

.data-table tr:last-child td {{
    border-bottom: none;
}}

/* Badges */
.badge {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 6px;
    font-size: 0.73rem;
    font-weight: 600;
}}
.badge-green {{ color: var(--green); background: var(--green-muted); border: 1px solid rgba(34, 197, 94, 0.25); }}
.badge-red {{ color: var(--red); background: var(--red-muted); border: 1px solid rgba(239, 68, 68, 0.25); }}
.badge-amber {{ color: var(--amber); background: var(--amber-muted); border: 1px solid rgba(245, 158, 11, 0.25); }}
.badge-blue {{ color: var(--accent); background: var(--accent-muted); border: 1px solid rgba(37, 99, 235, 0.25); }}
.badge-purple {{ color: var(--purple); background: var(--purple-muted); border: 1px solid rgba(168, 85, 247, 0.25); }}

/* Inputs & Form Controls */
.stButton > button {{
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: 0.83rem !important;
    border: 1px solid var(--border) !important;
    background: var(--card) !important;
    color: var(--text) !important;
    transition: all 0.15s ease !important;
}}
.stButton > button:hover {{
    border-color: var(--accent) !important;
    background: var(--card-hover) !important;
}}

div[data-baseweb="select"] {{
    background-color: var(--card) !important;
    border-radius: 8px !important;
}}
</style>
"""

st.markdown(CSS, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 4. Service Initializer & Plotly Theme Constants
# -----------------------------------------------------------------------------
@st.cache_resource
def get_service():
    return DataService()

service = get_service()

@st.cache_data(ttl=3600)
def fetch_sales_kpis(year, country):
    return service.get_sales_kpis(year=year, country_filter=country)

@st.cache_data(ttl=3600)
def fetch_sales_trends(year, country):
    return service.get_sales_trends(year=year, country_filter=country)

@st.cache_data(ttl=3600)
def fetch_day_and_hour():
    return service.get_sales_by_day_and_hour()

@st.cache_data(ttl=3600)
def fetch_top_products(limit, order_by, country):
    return service.get_top_products_sales(limit=limit, order_by=order_by, country_filter=country)

@st.cache_data(ttl=3600)
def fetch_price_tiers():
    return service.get_product_price_tiers()

@st.cache_data(ttl=3600)
def fetch_customer_segments():
    return service.get_customer_segments()

@st.cache_data(ttl=3600)
def fetch_geographic_sales():
    return service.get_geographic_sales()

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans, sans-serif", color="#a1a1aa" if IS_DARK else "#64748b", size=11),
    margin=dict(l=10, r=10, t=20, b=10),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        zerolinecolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        tickfont=dict(size=10, color="#a1a1aa" if IS_DARK else "#64748b"),
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        zerolinecolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        tickfont=dict(size=10, color="#a1a1aa" if IS_DARK else "#64748b"),
    ),
)

def metric_card(label: str, value: str, delta: str = None, delta_type: str = "up"):
    arrow = "↑" if delta_type == "up" else ("↓" if delta_type == "down" else "●")
    delta_html = f'<div class="metric-delta delta-{delta_type}">{arrow} {delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 5. Header Component & Global Filter Bar
# -----------------------------------------------------------------------------
head_col1, head_col2 = st.columns([10, 2])

with head_col1:
    st.markdown("""
    <div class="brand-container">
        <div>
            <div class="brand-title">
                <span>🛒 E-Commerce Sales & Revenue Intelligence</span>
                <span class="brand-badge">Live Store DW</span>
            </div>
            <div class="brand-desc">
                Real-Time Sales Performance • Customer Lifetime Value • Order Fulfillment & Merchandise Analytics
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

with head_col2:
    mode_label = "☀️ Light Mode" if IS_DARK else "🌙 Dark Mode"
    if st.button(mode_label, use_container_width=True):
        toggle_theme()
        st.rerun()

# Global Quick Filter Bar
filt_c1, filt_c2, filt_c3 = st.columns([4, 4, 4])
with filt_c1:
    year_choice = st.selectbox("📅 Sales Horizon:", ["All Historical (2010 - 2011)", "Fiscal Year 2011", "Fiscal Year 2010"])
with filt_c2:
    region_choice = st.selectbox("🌍 Sales Region:", ["Global (All Markets)", "United Kingdom Only", "International Exports Only"])
with filt_c3:
    st.markdown("<div style='height:28px;'></div>", unsafe_allow_html=True)
    st.caption("⚡ Ingested & Curated via E-Commerce Data Pipeline (536k transactions)")

# Resolve Filter Parameters
selected_year = None
if "2011" in year_choice:
    selected_year = 2011
elif "2010" in year_choice:
    selected_year = 2010

country_filter_val = "Global"
if "United Kingdom" in region_choice:
    country_filter_val = "UK"
elif "International" in region_choice:
    country_filter_val = "International"

# -----------------------------------------------------------------------------
# 6. E-Commerce Sales Dashboard Tabs
# -----------------------------------------------------------------------------
tab_overview, tab_products, tab_customers, tab_geo, tab_orders = st.tabs([
    "📊 Executive Sales Overview",
    "🛍️ Products & Merchandise",
    "👥 Customer Value & Segments",
    "🌍 Regional & Export Sales",
    "🧾 Sales Orders Explorer"
])

# =============================================================================
# TAB 1: Executive Sales Overview
# =============================================================================
with tab_overview:
    kpis = fetch_sales_kpis(year=selected_year, country=country_filter_val)

    # Row 1: Primary Revenue & Sales Volume Metrics
    r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
    with r1_c1:
        net_rev_fmt = f"£{kpis['net_sales']:,.2f}"
        metric_card("Net Realized Sales", net_rev_fmt, "After returns & refunds", "up")
    with r1_c2:
        gross_rev_fmt = f"£{kpis['gross_sales']:,.2f}"
        metric_card("Gross Sales Volume", gross_rev_fmt, f"{kpis['total_records']:,} transaction lines", "up")
    with r1_c3:
        orders_fmt = f"{kpis['valid_orders']:,}"
        metric_card("Fulfillable Orders", orders_fmt, f"{kpis['total_orders']:,} total invoices", "info")
    with r1_c4:
        aov_fmt = f"£{kpis['aov']:.2f}"
        metric_card("Average Order Value (AOV)", aov_fmt, "Gross revenue per order", "up")

    st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

    # Row 2: Secondary Operational Metrics
    r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
    with r2_c1:
        units_fmt = f"{kpis['units_sold']:,}"
        metric_card("Physical Units Shipped", units_fmt, "Total inventory volume", "up")
    with r2_c2:
        custs_fmt = f"{kpis['active_customers']:,}"
        metric_card("Active Registered Buyers", custs_fmt, "Distinct customer accounts", "info")
    with r2_c3:
        return_fmt = f"{kpis['return_rate_pct']:.2f}%"
        refund_fmt = f"£{kpis['cancellation_amount']:,.2f}"
        metric_card("Order Cancellation Rate", return_fmt, f"{refund_fmt} refunded", "down" if kpis['return_rate_pct'] > 5 else "warn")
    with r2_c4:
        guest_sales_fmt = f"£{kpis['guest_sales']:,.2f}"
        guest_pct = (kpis['guest_sales'] / kpis['gross_sales'] * 100) if kpis['gross_sales'] > 0 else 0
        metric_card("Guest Checkout Sales", guest_sales_fmt, f"{guest_pct:.1f}% anonymous revenue", "info")

    st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)

    # Revenue Trajectory & Monthly Order Volume
    trend_df = fetch_sales_trends(year=selected_year, country=country_filter_val)
    if not trend_df.empty:
        col_chart1, col_chart2 = st.columns([7, 5])
        with col_chart1:
            st.markdown("""
            <div class="chart-wrap">
                <div class="chart-header">
                    <div>
                        <div class="chart-title">Net Sales Revenue Trajectory (£)</div>
                        <div class="chart-subtitle">Monthly revenue realization across the retail calendar</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            fig_rev = px.area(
                trend_df,
                x="period_ym",
                y="net_sales",
                color_discrete_sequence=["#2563eb"],
            )
            fig_rev.update_layout(**PLOT_LAYOUT, height=290)
            fig_rev.update_traces(
                fillcolor="rgba(37, 99, 235, 0.16)",
                line=dict(width=2.5, color="#2563eb")
            )
            st.plotly_chart(fig_rev, use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)

        with col_chart2:
            st.markdown("""
            <div class="chart-wrap">
                <div class="chart-header">
                    <div>
                        <div class="chart-title">Monthly Orders & Average Order Value (AOV)</div>
                        <div class="chart-subtitle">Order frequency vs basket expenditure</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            fig_aov = go.Figure()
            fig_aov.add_trace(go.Bar(
                x=trend_df["period_ym"],
                y=trend_df["total_orders"],
                name="Orders Placed",
                marker_color="#22c55e" if IS_DARK else "#16a34a",
                yaxis="y"
            ))
            fig_aov.add_trace(go.Scatter(
                x=trend_df["period_ym"],
                y=trend_df["avg_order_value"],
                name="AOV (£)",
                marker_color="#f59e0b",
                yaxis="y2",
                mode="lines+markers",
                line=dict(width=2.5)
            ))
            layout_dual = dict(**PLOT_LAYOUT)
            layout_dual["yaxis2"] = dict(
                overlaying="y",
                side="right",
                showgrid=False,
                tickfont=dict(size=10, color="#f59e0b"),
            )
            layout_dual["height"] = 290
            layout_dual["legend"] = dict(orientation="h", y=1.05, x=0.5, xanchor="center", font=dict(size=10))
            fig_aov.update_layout(layout_dual)
            st.plotly_chart(fig_aov, use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)

    # Retail Shopping Behavior (Day of Week & Hourly Peaks)
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    timing_data = fetch_day_and_hour()
    timing_col1, timing_col2 = st.columns(2)

    with timing_col1:
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Sales Revenue by Day of the Week</div>
                    <div class="chart-subtitle">Customer order frequency across operational shopping days</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        fig_day = px.bar(
            timing_data["by_day"],
            x="day_name",
            y="total_sales",
            color_discrete_sequence=["#3b82f6"],
        )
        fig_day.update_layout(**PLOT_LAYOUT, height=240)
        st.plotly_chart(fig_day, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

    with timing_col2:
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Peak Purchasing Hours (Time of Day)</div>
                    <div class="chart-subtitle">Hourly customer shopping patterns (24-hour UTC clock)</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        fig_hr = px.line(
            timing_data["by_hour"],
            x="order_hour",
            y="order_count",
            markers=True,
            color_discrete_sequence=["#a855f7"],
        )
        fig_hr.update_layout(**PLOT_LAYOUT, height=240)
        fig_hr.update_traces(line=dict(width=2.5))
        st.plotly_chart(fig_hr, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# TAB 2: Products & Merchandise
# =============================================================================
with tab_products:
    prod_c1, prod_c2 = st.columns([3, 9])
    with prod_c1:
        prod_rank_by = st.selectbox("Rank Merchandise By:", ["Gross Revenue (£)", "Units Sold (Volume)"])
        prod_depth = st.slider("Leaderboard Size:", min_value=5, max_value=25, value=10, step=5)

    rank_param = "revenue" if "Revenue" in prod_rank_by else "quantity"
    top_merch_df = fetch_top_products(limit=prod_depth, order_by=rank_param, country=country_filter_val)

    m_col1, m_col2 = st.columns([6, 6])
    with m_col1:
        st.markdown(f"""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Top {prod_depth} Best-Selling Products</div>
                    <div class="chart-subtitle">Ranked by {rank_param}</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        metric_col = "total_revenue" if rank_param == "revenue" else "total_units"
        fig_merch = px.bar(
            top_merch_df.sort_values(metric_col, ascending=True),
            x=metric_col,
            y="description",
            orientation="h",
            color_discrete_sequence=["#2563eb"],
        )
        fig_merch.update_layout(**PLOT_LAYOUT, height=360)
        fig_merch.update_layout(yaxis=dict(autorange="reversed", tickfont=dict(size=9)))
        st.plotly_chart(fig_merch, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

    with m_col2:
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Merchandise Performance Metrics</div>
                    <div class="chart-subtitle">Unit movement, gross revenue, and invoice frequency</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        rows_html = ""
        for _, r in top_merch_df.iterrows():
            desc = html.escape(str(r["description"])[:32])
            code = html.escape(str(r["stock_code"]))
            rows_html += f"""
            <tr>
                <td><span class="mono" style="font-weight:600;">{code}</span></td>
                <td>{desc}</td>
                <td style="text-align:right;">{r['total_units']:,}</td>
                <td style="text-align:right; font-weight:600;">£{r['total_revenue']:,.2f}</td>
                <td style="text-align:right;">£{r['avg_unit_price']:.2f}</td>
            </tr>
            """

        table_markup = f"""
        <div class="data-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Stock Code</th>
                        <th>Product Description</th>
                        <th style="text-align:right;">Units</th>
                        <th style="text-align:right;">Gross Sales</th>
                        <th style="text-align:right;">Avg Price</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """
        st.markdown(table_markup, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    # Price Tier Breakdown & Catalog Search
    st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)
    tier_col, search_col = st.columns([5, 7])

    with tier_col:
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Revenue Distribution by Price Tier</div>
                    <div class="chart-subtitle">Gross sales contribution by individual item price range</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        tier_df = fetch_price_tiers()
        fig_tier = px.pie(
            tier_df,
            values="total_revenue",
            names="price_tier",
            hole=0.45,
            color_discrete_sequence=["#3b82f6", "#22c55e", "#a855f7", "#f59e0b"],
        )
        fig_tier.update_layout(**PLOT_LAYOUT, height=320)
        fig_tier.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_tier, use_container_width=True, config={"displayModeBar": False})
        st.markdown("</div>", unsafe_allow_html=True)

    with search_col:
        st.markdown("""
        <div class="chart-wrap">
            <div class="chart-header">
                <div>
                    <div class="chart-title">Product Catalog Search & Analysis</div>
                    <div class="chart-subtitle">Drill down into individual product sales and lifecycle metrics</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

        search_query = st.text_input("🔍 Search by Product Keyword or Stock Code:", value="HEART")
        if search_query:
            search_results = service.search_product(search_query)
            if not search_results.empty:
                st.dataframe(
                    search_results[[
                        "stock_code", "description", "total_units", "unit_price",
                        "estimated_revenue", "first_sale", "latest_sale"
                    ]],
                    use_container_width=True,
                    height=240
                )
            else:
                st.info(f"No products found matching '{search_query}'.")

        st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# TAB 3: Customer Value & Segments
# =============================================================================
with tab_customers:
    cust_data = fetch_customer_segments()
    tiers_df = cust_data["tiers"]
    top_vips = cust_data["top_vips"]

    st.markdown("""
    <div class="chart-wrap">
        <div class="chart-header">
            <div>
                <div class="chart-title">Customer Segmentation & Lifetime Value (CLV) Tiers</div>
                <div class="chart-subtitle">Pareto analysis: Value contribution across spending brackets</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    tier_c1, tier_c2 = st.columns([6, 6])
    with tier_c1:
        fig_clv = px.bar(
            tiers_df,
            x="spend_tier",
            y="tier_revenue",
            color="spend_tier",
            color_discrete_sequence=["#a855f7", "#3b82f6", "#22c55e", "#f59e0b"],
        )
        fig_clv.update_layout(**PLOT_LAYOUT, height=280, showlegend=False)
        st.plotly_chart(fig_clv, use_container_width=True, config={"displayModeBar": False})

    with tier_c2:
        tier_table_rows = ""
        for _, r in tiers_df.iterrows():
            tier_table_rows += f"""
            <tr>
                <td style="font-weight:600;">{r['spend_tier']}</td>
                <td style="text-align:right;">{r['customer_count']:,}</td>
                <td style="text-align:right; font-weight:600;">£{r['tier_revenue']:,.2f}</td>
                <td style="text-align:right;">{r['avg_orders_per_customer']} orders</td>
            </tr>
            """

        tier_markup = f"""
        <div class="data-table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Customer Tier</th>
                        <th style="text-align:right;">Accounts</th>
                        <th style="text-align:right;">Cumulative Spend</th>
                        <th style="text-align:right;">Avg Orders</th>
                    </tr>
                </thead>
                <tbody>
                    {tier_table_rows}
                </tbody>
            </table>
        </div>
        """
        st.markdown(tier_markup, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # Top VIP Customers Table
    st.markdown("""
    <div class="chart-wrap">
        <div class="chart-header">
            <div>
                <div class="chart-title">Top 10 VIP Accounts (Highest Lifetime Value)</div>
                <div class="chart-subtitle">Key wholesale and high-volume commercial accounts</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    vip_rows = ""
    for idx, r in top_vips.iterrows():
        vip_rows += f"""
        <tr>
            <td><span class="mono" style="font-weight:600;">#{int(r['customer_id'])}</span></td>
            <td>{r['country']}</td>
            <td style="text-align:right;">{r['total_orders']:,}</td>
            <td style="text-align:right; font-weight:600; color:var(--green);">£{r['total_spend']:,.2f}</td>
            <td style="text-align:right;">£{r['avg_order_value']:,.2f}</td>
            <td>{str(r['first_order_date'])[:10]}</td>
            <td>{str(r['last_order_date'])[:10]}</td>
        </tr>
        """

    vip_html = f"""
    <div class="data-table-wrap">
        <table class="data-table">
            <thead>
                <tr>
                    <th>Customer ID</th>
                    <th>Country</th>
                    <th style="text-align:right;">Orders</th>
                    <th style="text-align:right;">Lifetime Spend</th>
                    <th style="text-align:right;">Average Basket</th>
                    <th>First Purchase</th>
                    <th>Latest Purchase</th>
                </tr>
            </thead>
            <tbody>
                {vip_rows}
            </tbody>
        </table>
    </div>
    """
    st.markdown(vip_html, unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# TAB 4: Regional & Export Sales
# =============================================================================
with tab_geo:
    geo_df = fetch_geographic_sales()

    if not geo_df.empty:
        uk_row = geo_df[geo_df["country"] == "United Kingdom"]
        uk_rev = uk_row["gross_sales"].values[0] if not uk_row.empty else 0
        total_rev = geo_df["gross_sales"].sum()
        intl_rev = total_rev - uk_rev
        uk_pct = (uk_rev / total_rev * 100) if total_rev > 0 else 0
        intl_pct = 100 - uk_pct

        g1, g2, g3, g4 = st.columns(4)
        with g1:
            metric_card("Domestic Sales (UK)", f"£{uk_rev:,.2f}", f"{uk_pct:.1f}% total market", "up")
        with g2:
            metric_card("International Exports", f"£{intl_rev:,.2f}", f"{intl_pct:.1f}% cross-border sales", "up")
        with g3:
            metric_card("Global Markets Served", f"{len(geo_df)} Countries", "Export footprint", "info")
        with g4:
            top_intl_market = geo_df[geo_df["country"] != "United Kingdom"].iloc[0]
            metric_card(f"Top Export: {top_intl_market['country']}", f"£{top_intl_market['gross_sales']:,.2f}", f"{top_intl_market['total_orders']:,} orders", "up")

        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

        geo_c1, geo_c2 = st.columns([6, 6])
        with geo_c1:
            st.markdown("""
            <div class="chart-wrap">
                <div class="chart-header">
                    <div>
                        <div class="chart-title">Top 10 International Export Markets (Excl. UK)</div>
                        <div class="chart-subtitle">Gross revenue across major foreign buyer markets</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            top_intl_chart_df = geo_df[geo_df["country"] != "United Kingdom"].head(10)
            fig_geo = px.bar(
                top_intl_chart_df.sort_values("gross_sales", ascending=True),
                x="gross_sales",
                y="country",
                orientation="h",
                color_discrete_sequence=["#22c55e" if IS_DARK else "#16a34a"],
            )
            fig_geo.update_layout(**PLOT_LAYOUT, height=360)
            st.plotly_chart(fig_geo, use_container_width=True, config={"displayModeBar": False})
            st.markdown("</div>", unsafe_allow_html=True)

        with geo_c2:
            st.markdown("""
            <div class="chart-wrap">
                <div class="chart-header">
                    <div>
                        <div class="chart-title">Country Performance Matrix</div>
                        <div class="chart-subtitle">Complete volume and revenue breakdown by destination</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

            geo_table_rows = ""
            for _, r in geo_df.head(15).iterrows():
                geo_table_rows += f"""
                <tr>
                    <td style="font-weight:600;">{r['country']}</td>
                    <td style="text-align:right;">{r['total_orders']:,}</td>
                    <td style="text-align:right;">{r['total_units']:,}</td>
                    <td style="text-align:right; font-weight:600;">£{r['gross_sales']:,.2f}</td>
                    <td style="text-align:right;">£{r['avg_order_value']:.2f}</td>
                </tr>
                """

            geo_matrix_html = f"""
            <div class="data-table-wrap" style="max-height: 380px;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Country</th>
                            <th style="text-align:right;">Orders</th>
                            <th style="text-align:right;">Units</th>
                            <th style="text-align:right;">Gross (£)</th>
                            <th style="text-align:right;">Avg Order</th>
                        </tr>
                    </thead>
                    <tbody>
                        {geo_table_rows}
                    </tbody>
                </table>
            </div>
            """
            st.markdown(geo_matrix_html, unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)


# =============================================================================
# TAB 5: Sales Orders Explorer
# =============================================================================
with tab_orders:
    st.markdown("""
    <div class="chart-wrap">
        <div class="chart-header">
            <div>
                <div class="chart-title">Live Sales Orders & Line Item Explorer</div>
                <div class="chart-subtitle">Search, filter, and inspect transaction records stored in the Data Warehouse</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    o_c1, o_c2, o_c3, o_c4 = st.columns([4, 3, 3, 2])
    with o_c1:
        query_inp = st.text_input("Search Orders:", placeholder="Invoice #, Customer ID, Item description...")
    with o_c2:
        status_inp = st.selectbox("Order Status:", ["All Orders", "Completed Sales", "Cancelled / Returned"])
    with o_c3:
        all_countries = ["Global"] + sorted(geo_df["country"].dropna().unique().tolist()) if not geo_df.empty else ["Global"]
        country_inp = st.selectbox("Destination Country:", all_countries)
    with o_c4:
        limit_inp = st.selectbox("Fetch Rows:", [25, 50, 100, 200])

    orders_df = service.search_orders(
        query_text=query_inp if query_inp else None,
        country_filter=country_inp if country_inp != "Global" else None,
        status_filter=status_inp if status_inp != "All Orders" else None,
        limit=limit_inp
    )

    if not orders_df.empty:
        summary_c1, summary_c2, summary_c3 = st.columns(3)
        with summary_c1:
            st.caption(f"Retrieved **{len(orders_df)}** records")
        with summary_c2:
            st.caption(f"Filtered Gross Sales: **£{orders_df['gross_amount'].sum():,.2f}**")
        with summary_c3:
            csv_data = orders_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Export Filtered Sales (CSV)",
                data=csv_data,
                file_name="filtered_ecommerce_sales.csv",
                mime="text/csv",
                use_container_width=True
            )

        st.dataframe(orders_df, use_container_width=True, height=450)
    else:
        st.info("No matching sales orders found for the given criteria.")

    st.markdown("</div>", unsafe_allow_html=True)
