"""
ICICI Prudential MF Flow Dashboard (v2)
========================================
Streamlit app showing net monthly flows for ICICI Pru equity & hybrid schemes.
Time-series first approach with period aggregation (Monthly/Quarterly/FY/FY YTD).

Launch: streamlit run app.py --server.port 8501
"""

import os, sqlite3, numpy as np, pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date
from dateutil.relativedelta import relativedelta

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ICICI Pru MF Flows",
    page_icon="\U0001f4ca",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys
sys.path.insert(0, os.path.dirname(__file__))
import pipeline as pl

DB_PATH = pl.DB_PATH

# ── Styling ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; font-size: 16px; }
    .kpi-card {
        background: #ffffff; border: 1px solid #e0e0e0; border-radius: 10px;
        padding: 18px 20px; text-align: center;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    .kpi-label { font-size: 12px; font-weight: 600; color: #6b7280;
                 text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 6px; }
    .kpi-value { font-size: 28px; font-weight: 700; color: #1f2937; }
    .kpi-sub   { font-size: 12px; color: #9ca3af; margin-top: 4px; }
    .kpi-pos   { color: #16a34a !important; }
    .kpi-neg   { color: #dc2626 !important; }
    .section-header {
        font-size: 18px; font-weight: 600; color: #1f2937;
        border-left: 3px solid #2563eb; padding-left: 10px;
        margin: 24px 0 16px 0;
    }
</style>
""", unsafe_allow_html=True)

CHART_THEME = dict(paper_bgcolor="#ffffff", plot_bgcolor="#fafafa",
                   font=dict(color="#1f2937", size=13))
AXIS_STYLE = dict(gridcolor="#e5e7eb", linecolor="#d1d5db")
COLOR_POS = "#16a34a"
COLOR_NEG = "#dc2626"
COLOR_AUM = "#2563eb"


# ── Period helpers ───────────────────────────────────────────────────────────
def assign_fy(dt):
    """Indian FY: Apr-Mar. FY26 = Apr 2025 to Mar 2026."""
    if dt.month >= 4:
        return f"FY{(dt.year + 1) % 100:02d}"
    return f"FY{dt.year % 100:02d}"


def assign_quarter(dt):
    m = dt.month
    if m in (4, 5, 6):
        q = "Q1"
    elif m in (7, 8, 9):
        q = "Q2"
    elif m in (10, 11, 12):
        q = "Q3"
    else:
        q = "Q4"
    return f"{q} {assign_fy(dt)}"


def get_current_fy():
    t = date.today()
    if t.month >= 4:
        return f"FY{(t.year + 1) % 100:02d}"
    return f"FY{t.year % 100:02d}"


def add_period_cols(df):
    df = df.copy()
    df["fy"] = df["month_end"].apply(assign_fy)
    df["quarter"] = df["month_end"].apply(assign_quarter)
    df["month_lbl"] = df["month_end"].dt.strftime("%b '%y")
    return df


def agg_by_period(df, period, extra_group=None):
    """
    Aggregate monthly data to selected period granularity.
    Two-step: (1) sum schemes → monthly totals, (2) aggregate months → period.
    """
    if extra_group is None:
        extra_group = []
    df = df.sort_values("month_end").copy()

    if period == "Monthly":
        df["_period"] = df["month_lbl"]
    elif period == "Quarterly":
        df["_period"] = df["quarter"]
    elif period == "Financial Year":
        df["_period"] = df["fy"]
    elif period == "FY YTD":
        cfy = get_current_fy()
        df = df[df["fy"] == cfy]
        df["_period"] = df["month_lbl"]

    if df.empty:
        return pd.DataFrame()

    # Step 1: Sum scheme-level data to monthly totals (per extra_group)
    monthly_grp = extra_group + ["_period", "month_end"]
    monthly = df.groupby(monthly_grp, sort=False).agg(
        net_flow_cr=("net_flow_cr", "sum"),
        aum_cr=("aum_cur_cr", "sum"),
    ).reset_index().sort_values("month_end")

    # Step 2: Aggregate monthly totals to period level
    #   Flows: SUM across months in the period
    #   AUM: last month's total (end-of-period AUM)
    grp = extra_group + ["_period"]
    agg = monthly.groupby(grp, sort=False).agg(
        net_flow_cr=("net_flow_cr", "sum"),
        aum_cr=("aum_cr", "last"),
        period_sort=("month_end", "max"),
    ).reset_index()
    agg.rename(columns={"_period": "period_label"}, inplace=True)
    agg["flow_pct"] = np.where(
        agg["aum_cr"] > 0, agg["net_flow_cr"] / agg["aum_cr"] * 100, 0
    )
    agg["flow_pct"] = agg["flow_pct"].round(1)
    return agg.sort_values("period_sort")


# ── Helpers ──────────────────────────────────────────────────────────────────
def fmt_cr(val):
    if pd.isna(val):
        return "\u2014"
    if abs(val) >= 1e5:
        return f"\u20b9{val/1e5:,.1f}L Cr"
    if abs(val) >= 1e3:
        return f"\u20b9{val/1e3:,.1f}K Cr"
    return f"\u20b9{val:,.0f} Cr"


def kpi(label, value, sub="", css_class=""):
    return f"""<div class='kpi-card'>
        <div class='kpi-label'>{label}</div>
        <div class='kpi-value {css_class}'>{value}</div>
        <div class='kpi-sub'>{sub}</div>
    </div>"""


def db_exists():
    try:
        con = sqlite3.connect(DB_PATH)
        c = con.execute("SELECT COUNT(*) FROM monthly_flows").fetchone()[0]
        con.close()
        return c > 0
    except Exception:
        return False


@st.cache_data(ttl=3600)
def load_data():
    df = pl.load_flows(months=36)
    if df.empty:
        return df
    df["month_end"] = pd.to_datetime(df["month_end"])
    df = add_period_cols(df)
    return df


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## \U0001f4ca ICICI Pru MF Flows")
    st.markdown("---")

    st.markdown("### \U0001f4e5 Fetch New Data")
    st.caption("Select a month to fetch from AMFI and compute flows.")
    col_y, col_m = st.columns(2)
    with col_y:
        sel_year = st.selectbox("Year", list(range(2024, date.today().year + 1))[::-1])
    with col_m:
        sel_month = st.selectbox("Month", list(range(1, 13)),
                                  format_func=lambda m: pl.MONTH_ABBR[m])

    if st.button("\u2b07\ufe0f Fetch & Compute", use_container_width=True):
        with st.spinner(f"Fetching data for {pl.MONTH_ABBR[sel_month]} {sel_year}..."):
            try:
                pl.compute_flows_for_month(sel_year, sel_month)
                st.cache_data.clear()
                st.success("Done! Data updated.")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.expander("\U0001f4c5 Load Multiple Months", expanded=False):
        st.caption("Fetch data for several past months at once.")
        hist_months = st.number_input("Months to go back", min_value=1, max_value=24, value=6)

        preview_list = []
        for i in range(hist_months, -1, -1):
            dt = date(sel_year, sel_month, 1) - relativedelta(months=i)
            preview_list.append(f"{pl.MONTH_ABBR[dt.month]} {dt.year}")
        st.info(f"Will fetch: **{preview_list[0]}** to **{preview_list[-1]}** ({len(preview_list)} months)")

        if st.button("\U0001f504 Load Historical Data", use_container_width=True):
            import time
            progress = st.progress(0)
            months_list = []
            for i in range(hist_months, -1, -1):
                dt = date(sel_year, sel_month, 1) - relativedelta(months=i)
                months_list.append((dt.year, dt.month))
            for idx, (yr, mn) in enumerate(months_list):
                st.text(f"Processing {pl.MONTH_ABBR[mn]} {yr}...")
                try:
                    pl.compute_flows_for_month(yr, mn)
                except Exception as e:
                    st.warning(f"Error for {pl.MONTH_ABBR[mn]} {yr}: {e}")
                progress.progress((idx + 1) / len(months_list))
                time.sleep(0.5)
            st.cache_data.clear()
            st.success(f"Loaded {len(months_list)} months!")

    st.markdown("---")
    st.markdown("### \U0001f50d Filters")
    category_filter = st.multiselect("Category", ["Equity", "Hybrid"],
                                      default=["Equity", "Hybrid"])

    st.markdown("---")
    st.caption(
        "**Data Source**: AMFI-CRISIL Fund Performance API\n\n"
        "**Formula**: Flow = AUM(t) \u2212 AUM(t\u22121) \u00d7 NAV(t)/NAV(t\u22121)\n\n"
        "Uses Regular Plan Growth NAV as MTM benchmark"
    )


# ── Main ─────────────────────────────────────────────────────────────────────
st.markdown("# ICICI Prudential \u2014 Mutual Fund Flow Dashboard")
st.markdown(
    "*Net flows = Actual AUM \u2212 Expected AUM, where "
    "Expected AUM = Prior month AUM \u00d7 (Current NAV / Prior NAV)*"
)

if not db_exists():
    st.info("""
    ### No data loaded yet
    Use the **sidebar** to select a year/month and click **Fetch & Compute** to load data.
    """)
    st.stop()

df = load_data()
if df.empty:
    st.warning("No data available. Try fetching data using the sidebar.")
    st.stop()

df = df[df["category"].isin(category_filter)]
if df.empty:
    st.warning("No data matches your filters.")
    st.stop()


# ── Period Selector ──────────────────────────────────────────────────────────
period = st.radio(
    "View Period",
    ["Monthly", "Quarterly", "Financial Year", "FY YTD"],
    horizontal=True,
    index=0,
)

# Latest month for reference
latest_month = df["month_end"].max()
latest_month_lbl = pd.Timestamp(latest_month).strftime("%B %Y")

# FY YTD
cfy = get_current_fy()
df_fy_ytd = df[df["fy"] == cfy]
fy_ytd_flow = df_fy_ytd["net_flow_cr"].sum()
fy_ytd_months = df_fy_ytd["month_end"].nunique()

# Latest month stats
df_latest = df[df["month_end"] == latest_month]
total_flow = df_latest["net_flow_cr"].sum()
total_aum = df_latest["aum_cur_cr"].sum()
num_schemes = df_latest["scheme_name"].nunique()
inflow_schemes = (df_latest["net_flow_cr"] > 0).sum()
outflow_schemes = (df_latest["net_flow_cr"] < 0).sum()
flow_pct = (total_flow / total_aum * 100) if total_aum else 0


# ── KPI Row ──────────────────────────────────────────────────────────────────
st.markdown(
    f"<div class='section-header'>\U0001f4cc Snapshot \u2014 {latest_month_lbl}</div>",
    unsafe_allow_html=True,
)

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    cls = "kpi-pos" if total_flow >= 0 else "kpi-neg"
    st.markdown(kpi("Net Flow (Latest)", fmt_cr(total_flow), latest_month_lbl, cls),
                unsafe_allow_html=True)
with k2:
    cls = "kpi-pos" if fy_ytd_flow >= 0 else "kpi-neg"
    st.markdown(kpi(f"{cfy} YTD Net Flow", fmt_cr(fy_ytd_flow),
                     f"{fy_ytd_months} months", cls),
                unsafe_allow_html=True)
with k3:
    st.markdown(kpi("Total AUM", fmt_cr(total_aum), f"{num_schemes} schemes"),
                unsafe_allow_html=True)
with k4:
    st.markdown(kpi("Inflow Schemes", str(inflow_schemes),
                     "positive net flow", "kpi-pos"),
                unsafe_allow_html=True)
with k5:
    cls = "kpi-pos" if flow_pct >= 0 else "kpi-neg"
    st.markdown(kpi("Flow / AUM", f"{flow_pct:+.2f}%", "latest month", cls),
                unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "\U0001f4c8 Flow & AUM Trends",
    "\U0001f3c6 Scheme Breakdown",
    "\U0001f5fa\ufe0f Category Heatmap",
    "\U0001f4cb Raw Data",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — FLOW & AUM TRENDS (Time-Series First)
# ═══════════════════════════════════════════════════════════════════════
with tab1:
    # Aggregate to selected period
    monthly = agg_by_period(df, period)

    if monthly.empty:
        st.warning("No data for selected period.")
    else:
        # ── Chart 1: Net Flow bars + Flow/AUM % line ──────────────
        st.markdown(
            "<div class='section-header'>Net Inflows / Outflows</div>",
            unsafe_allow_html=True,
        )

        fig_flow = make_subplots(specs=[[{"secondary_y": True}]])
        flow_colors = [COLOR_POS if v >= 0 else COLOR_NEG for v in monthly["net_flow_cr"]]
        flow_labels = [fmt_cr(v) for v in monthly["net_flow_cr"]]

        fig_flow.add_trace(go.Bar(
            x=monthly["period_label"], y=monthly["net_flow_cr"],
            name="Net Flow", marker_color=flow_colors, opacity=0.85,
            text=flow_labels, textposition="outside", textfont=dict(size=11),
            hovertemplate="Net Flow: \u20b9%{y:,.0f} Cr<extra></extra>",
        ), secondary_y=False)

        fig_flow.add_trace(go.Scatter(
            x=monthly["period_label"], y=monthly["flow_pct"],
            name="Flow / AUM %", line=dict(color="#7c3aed", width=2.5),
            mode="lines+markers", marker=dict(size=6),
            hovertemplate="Flow/AUM: %{y:+.1f}%<extra></extra>",
        ), secondary_y=True)

        fig_flow.update_layout(
            height=460, barmode="relative",
            legend=dict(orientation="h", y=1.08, font=dict(size=12)),
            **CHART_THEME, hovermode="x",
            xaxis=dict(**AXIS_STYLE, tickfont=dict(size=12)),
            yaxis=dict(title="Net Flow (\u20b9 Cr)", zeroline=True,
                       zerolinecolor="#d1d5db", **AXIS_STYLE),
            yaxis2=dict(title="Flow / AUM (%)", gridcolor="rgba(0,0,0,0)",
                        ticksuffix="%", zeroline=True, zerolinecolor="#d1d5db"),
        )
        st.plotly_chart(fig_flow, use_container_width=True)

        # ── Chart 2: AUM bars + YoY Growth % ──────────────────────
        st.markdown(
            "<div class='section-header'>Total AUM & YoY Growth</div>",
            unsafe_allow_html=True,
        )

        aum_data = monthly.copy().reset_index(drop=True)
        aum_labels = [fmt_cr(v) for v in aum_data["aum_cr"]]

        # Compute YoY
        yoy = []
        for i, row in aum_data.iterrows():
            target = row["period_sort"] - pd.DateOffset(years=1)
            prev = aum_data[aum_data["period_sort"].between(
                target - pd.Timedelta(days=45), target + pd.Timedelta(days=45)
            )]
            if not prev.empty and prev.iloc[0]["aum_cr"] > 0:
                yoy.append(
                    (row["aum_cr"] - prev.iloc[0]["aum_cr"])
                    / prev.iloc[0]["aum_cr"] * 100
                )
            else:
                yoy.append(None)
        aum_data["yoy_pct"] = yoy

        fig_aum = make_subplots(specs=[[{"secondary_y": True}]])
        fig_aum.add_trace(go.Bar(
            x=aum_data["period_label"], y=aum_data["aum_cr"],
            name="AUM", marker_color=COLOR_AUM, opacity=0.85,
            text=aum_labels, textposition="outside", textfont=dict(size=11),
            hovertemplate="AUM: \u20b9%{y:,.0f} Cr<extra></extra>",
        ), secondary_y=False)

        fig_aum.add_trace(go.Scatter(
            x=aum_data["period_label"], y=aum_data["yoy_pct"],
            name="YoY AUM Growth %", line=dict(color="#f59e0b", width=2.5),
            mode="lines+markers", marker=dict(size=6), connectgaps=True,
            hovertemplate="YoY: %{y:+.1f}%<extra></extra>",
        ), secondary_y=True)

        fig_aum.update_layout(
            height=460, barmode="relative",
            legend=dict(orientation="h", y=1.08, font=dict(size=12)),
            **CHART_THEME, hovermode="x",
            xaxis=dict(**AXIS_STYLE, tickfont=dict(size=12)),
            yaxis=dict(title="AUM (\u20b9 Cr)", **AXIS_STYLE),
            yaxis2=dict(title="YoY Growth (%)", gridcolor="rgba(0,0,0,0)",
                        ticksuffix="%"),
        )
        st.plotly_chart(fig_aum, use_container_width=True)

    # ── Category stacked bar ──────────────────────────────────────
    st.markdown(
        "<div class='section-header'>Flow Breakdown by Sub-Category</div>",
        unsafe_allow_html=True,
    )

    cat_agg = agg_by_period(df, period, extra_group=["sub_category"])
    if not cat_agg.empty:
        cat_pivot = cat_agg.pivot_table(
            index="period_label", columns="sub_category",
            values="net_flow_cr", aggfunc="sum"
        ).fillna(0)
        order_map = cat_agg.drop_duplicates("period_label").set_index("period_label")["period_sort"]
        sorted_idx = order_map.sort_values().index.tolist()
        cat_pivot = cat_pivot.reindex([x for x in sorted_idx if x in cat_pivot.index])

        # Limit to top 10 categories by total absolute flow
        cat_totals = cat_pivot.abs().sum().nlargest(10).index.tolist()
        cat_pivot = cat_pivot[cat_totals]

        palette = px.colors.qualitative.Set2
        fig2 = go.Figure()
        for i, col in enumerate(cat_pivot.columns):
            fig2.add_trace(go.Bar(
                x=cat_pivot.index, y=cat_pivot[col], name=col,
                marker_color=palette[i % len(palette)],
                hovertemplate=f"<b>{col}</b>: " + "\u20b9%{y:,.0f} Cr<extra></extra>",
            ))
        fig2.update_layout(
            barmode="relative", height=440,
            **CHART_THEME, hovermode="x",
            legend=dict(orientation="h", y=-0.25, font=dict(size=11), traceorder="normal"),
            xaxis=dict(title="", tickfont=dict(size=12), **AXIS_STYLE),
            yaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
        )
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — SCHEME BREAKDOWN (uses latest month)
# ═══════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown(
        f"<div class='section-header'>Scheme Breakdown \u2014 {latest_month_lbl}</div>",
        unsafe_allow_html=True,
    )

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("**\U0001f7e2 Top 10 Schemes by Inflow**")
        top_in = (
            df_latest.nlargest(10, "net_flow_cr")
            [["scheme_name", "net_flow_cr", "aum_cur_cr", "sub_category"]].copy()
        )
        top_in["short_name"] = top_in["scheme_name"].str.replace(
            "ICICI Prudential ", "", regex=False
        )
        sorted_in = top_in.sort_values("net_flow_cr")
        fig3 = go.Figure(go.Bar(
            x=sorted_in["net_flow_cr"], y=sorted_in["short_name"],
            orientation="h", marker_color=COLOR_POS,
            text=[fmt_cr(v) for v in sorted_in["net_flow_cr"]],
            textposition="outside", textfont=dict(size=11), cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Flow: \u20b9%{x:,.0f} Cr<extra></extra>",
        ))
        fig3.update_layout(
            height=420, **CHART_THEME, showlegend=False, margin=dict(r=100),
            xaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
            yaxis=dict(tickfont=dict(size=11), **AXIS_STYLE),
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_r:
        st.markdown("**\U0001f534 Top 10 Schemes by Outflow**")
        top_out = (
            df_latest.nsmallest(10, "net_flow_cr")
            [["scheme_name", "net_flow_cr", "aum_cur_cr", "sub_category"]].copy()
        )
        top_out["short_name"] = top_out["scheme_name"].str.replace(
            "ICICI Prudential ", "", regex=False
        )
        sorted_out = top_out.sort_values("net_flow_cr", ascending=False)
        fig4 = go.Figure(go.Bar(
            x=sorted_out["net_flow_cr"], y=sorted_out["short_name"],
            orientation="h", marker_color=COLOR_NEG,
            text=[fmt_cr(v) for v in sorted_out["net_flow_cr"]],
            textposition="outside", textfont=dict(size=11), cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Flow: \u20b9%{x:,.0f} Cr<extra></extra>",
        ))
        fig4.update_layout(
            height=420, **CHART_THEME, showlegend=False, margin=dict(l=100),
            xaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
            yaxis=dict(tickfont=dict(size=11), **AXIS_STYLE),
        )
        st.plotly_chart(fig4, use_container_width=True)

    # Major Schemes by AUM
    st.markdown(
        f"<div class='section-header'>All Major Schemes by AUM \u2014 "
        f"{latest_month_lbl}</div>",
        unsafe_allow_html=True,
    )

    top_aum = (
        df_latest.nlargest(20, "aum_cur_cr")
        [["scheme_name", "net_flow_cr", "aum_cur_cr", "sub_category"]].copy()
        .sort_values("aum_cur_cr")
    )
    top_aum["short_name"] = top_aum["scheme_name"].str.replace(
        "ICICI Prudential ", "", regex=False
    )

    fig_major = make_subplots(
        rows=1, cols=2, shared_yaxes=True,
        column_widths=[0.6, 0.4], horizontal_spacing=0.02,
        subplot_titles=("AUM (\u20b9 Cr)", "Net Flow (\u20b9 Cr)"),
    )
    fig_major.add_trace(go.Bar(
        x=top_aum["aum_cur_cr"], y=top_aum["short_name"],
        orientation="h", marker_color=COLOR_AUM, opacity=0.8,
        text=[fmt_cr(v) for v in top_aum["aum_cur_cr"]],
        textposition="outside", textfont=dict(size=10), cliponaxis=False,
        name="AUM",
        hovertemplate="<b>%{y}</b><br>AUM: \u20b9%{x:,.0f} Cr<extra></extra>",
    ), row=1, col=1)

    bar_colors_aum = [COLOR_POS if v >= 0 else COLOR_NEG for v in top_aum["net_flow_cr"]]
    fig_major.add_trace(go.Bar(
        x=top_aum["net_flow_cr"], y=top_aum["short_name"],
        orientation="h", marker_color=bar_colors_aum,
        text=[fmt_cr(v) for v in top_aum["net_flow_cr"]],
        textposition="outside", textfont=dict(size=10), cliponaxis=False,
        name="Net Flow",
        hovertemplate="<b>%{y}</b><br>Flow: \u20b9%{x:,.0f} Cr<extra></extra>",
    ), row=1, col=2)

    fig_major.update_layout(
        height=max(500, len(top_aum) * 30),
        **CHART_THEME, showlegend=False, margin=dict(r=100, l=10),
    )
    fig_major.update_xaxes(**AXIS_STYLE)
    fig_major.update_yaxes(tickfont=dict(size=11), **AXIS_STYLE)
    st.plotly_chart(fig_major, use_container_width=True)

    # Scatter: Flow vs AUM
    st.markdown(
        "<div class='section-header'>Flow vs AUM \u2014 Size = AUM, Color = Flow direction</div>",
        unsafe_allow_html=True,
    )
    scatter_df = df_latest.dropna(subset=["net_flow_cr", "aum_cur_cr"]).copy()
    scatter_df["flow_dir"] = scatter_df["net_flow_cr"].apply(
        lambda x: "Inflow" if x >= 0 else "Outflow"
    )
    scatter_df["short_name"] = scatter_df["scheme_name"].str.replace(
        "ICICI Prudential ", "", regex=False
    )

    fig5 = px.scatter(
        scatter_df, x="aum_cur_cr", y="net_flow_cr",
        color="flow_dir", size="aum_cur_cr", size_max=40, opacity=0.75,
        color_discrete_map={"Inflow": COLOR_POS, "Outflow": COLOR_NEG},
        hover_name="short_name",
        labels={"aum_cur_cr": "AUM (\u20b9 Cr)", "net_flow_cr": "Net Flow (\u20b9 Cr)"},
        height=440,
    )
    fig5.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>"
                      "AUM: \u20b9%{x:,.0f} Cr<br>"
                      "Flow: \u20b9%{y:,.0f} Cr<extra></extra>"
    )
    fig5.add_hline(y=0, line_dash="dash", line_color="#9ca3af", line_width=1)
    fig5.update_layout(
        **CHART_THEME,
        xaxis=dict(**AXIS_STYLE), yaxis=dict(**AXIS_STYLE),
        legend=dict(font=dict(size=13)),
    )
    st.plotly_chart(fig5, use_container_width=True)

    # Drill-down: Scheme flow history
    st.markdown(
        "<div class='section-header'>Drill-down: Scheme Flow History</div>",
        unsafe_allow_html=True,
    )
    all_schemes = sorted(df["scheme_name"].unique())
    sel_scheme = st.selectbox("Select Scheme", all_schemes)

    sch_df = df[df["scheme_name"] == sel_scheme]
    sch_agg = agg_by_period(sch_df, period)

    if not sch_agg.empty:
        fig6 = make_subplots(specs=[[{"secondary_y": True}]])
        bar_colors = [COLOR_POS if v >= 0 else COLOR_NEG for v in sch_agg["net_flow_cr"]]
        fig6.add_trace(
            go.Bar(x=sch_agg["period_label"], y=sch_agg["net_flow_cr"],
                   name="Net Flow", marker_color=bar_colors,
                   hovertemplate="Flow: \u20b9%{y:,.0f} Cr<extra></extra>"),
            secondary_y=False,
        )
        fig6.add_trace(
            go.Scatter(x=sch_agg["period_label"], y=sch_agg["aum_cr"],
                       name="AUM", line=dict(color=COLOR_AUM, width=2),
                       hovertemplate="AUM: \u20b9%{y:,.0f} Cr<extra></extra>"),
            secondary_y=True,
        )
        fig6.update_layout(
            height=380, **CHART_THEME, hovermode="x",
            xaxis=dict(tickfont=dict(size=12), **AXIS_STYLE),
            yaxis=dict(title="Flow (\u20b9 Cr)", **AXIS_STYLE),
            yaxis2=dict(title="AUM (\u20b9 Cr)"),
            legend=dict(font=dict(size=13)),
        )
        st.plotly_chart(fig6, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — CATEGORY HEATMAP
# ═══════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown(
        "<div class='section-header'>Flow Heatmap \u2014 Sub-category \u00d7 Period</div>",
        unsafe_allow_html=True,
    )
    st.caption("Red = outflows, Green = inflows.")

    cat_hm = agg_by_period(df, period, extra_group=["sub_category"])
    if not cat_hm.empty:
        hm_pivot = cat_hm.pivot_table(
            index="sub_category", columns="period_label",
            values="net_flow_cr", aggfunc="sum"
        ).fillna(0)

        order_map = cat_hm.drop_duplicates("period_label").set_index("period_label")["period_sort"]
        sorted_idx = order_map.sort_values().index.tolist()
        hm_pivot = hm_pivot.reindex(columns=[x for x in sorted_idx if x in hm_pivot.columns])

        if not hm_pivot.empty:
            fig7 = go.Figure(data=go.Heatmap(
                z=hm_pivot.values,
                x=hm_pivot.columns.tolist(),
                y=hm_pivot.index.tolist(),
                colorscale=[
                    [0.0, "#dc2626"], [0.4, "#fecaca"],
                    [0.5, "#f3f4f6"], [0.6, "#bbf7d0"], [1.0, "#16a34a"],
                ],
                zmid=0,
                text=[[fmt_cr(v) for v in row] for row in hm_pivot.values],
                texttemplate="%{text}",
                textfont=dict(size=10, color="#1f2937"),
                hovertemplate="<b>%{y}</b><br>%{x}: %{text}<extra></extra>",
                colorbar=dict(title="\u20b9 Cr", tickfont=dict(size=11)),
            ))
            fig7.update_layout(
                height=max(350, len(hm_pivot) * 48),
                **CHART_THEME,
                xaxis=dict(side="top", tickfont=dict(size=12)),
                yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            )
            st.plotly_chart(fig7, use_container_width=True)

    # Cumulative
    st.markdown(
        "<div class='section-header'>Cumulative Net Flow by Category (all data)</div>",
        unsafe_allow_html=True,
    )
    cum = df.groupby("sub_category")["net_flow_cr"].sum().sort_values()
    fig8 = go.Figure(go.Bar(
        x=cum.values, y=cum.index, orientation="h",
        marker_color=[COLOR_POS if v >= 0 else COLOR_NEG for v in cum.values],
        text=[fmt_cr(v) for v in cum.values],
        textposition="outside", textfont=dict(size=12), cliponaxis=False,
        hovertemplate="<b>%{y}</b>: \u20b9%{x:,.0f} Cr<extra></extra>",
    ))
    fig8.update_layout(
        height=440, **CHART_THEME, margin=dict(r=100),
        xaxis=dict(title="Cumulative Net Flow (\u20b9 Cr)", **AXIS_STYLE),
        yaxis=dict(tickfont=dict(size=12), **AXIS_STYLE),
    )
    st.plotly_chart(fig8, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — RAW DATA (all months)
# ═══════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown(
        "<div class='section-header'>Full Data \u2014 All Months Loaded</div>",
        unsafe_allow_html=True,
    )

    show_df = (
        df[[
            "month_lbl", "scheme_name", "category", "sub_category",
            "aum_cur_cr", "aum_prev_cr", "expected_aum_cr",
            "nav_cur", "nav_prev", "nav_return",
            "net_flow_cr", "flow_pct",
        ]].copy()
        .sort_values(["month_end", "net_flow_cr"], ascending=[False, False])
    )

    show_df.columns = [
        "Month", "Scheme", "Category", "Sub-Category",
        "AUM Cur (\u20b9Cr)", "AUM Prev (\u20b9Cr)", "Expected AUM (\u20b9Cr)",
        "NAV Cur", "NAV Prev", "NAV Return",
        "Net Flow (\u20b9Cr)", "Flow %",
    ]

    for col in ["AUM Cur (\u20b9Cr)", "AUM Prev (\u20b9Cr)", "Expected AUM (\u20b9Cr)",
                "Net Flow (\u20b9Cr)"]:
        show_df[col] = show_df[col].apply(lambda x: round(x, 0) if pd.notna(x) else None)
    show_df["NAV Return"] = show_df["NAV Return"].apply(
        lambda x: f"{x:.4f}" if pd.notna(x) else None
    )
    show_df["Flow %"] = show_df["Flow %"].apply(
        lambda x: f"{x:+.2f}%" if pd.notna(x) else None
    )

    st.dataframe(show_df, use_container_width=True, height=500)

    csv = show_df.to_csv(index=False)
    st.download_button("\u2b07\ufe0f Download CSV", csv, "icici_pru_flows.csv", "text/csv")

    # Pipeline log
    st.markdown(
        "<div class='section-header'>Pipeline run log</div>",
        unsafe_allow_html=True,
    )
    log_df = pl.load_pipeline_log()
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True)


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data: AMFI-CRISIL Fund Performance API  \u2022  "
    "Flows = Actual AUM \u2212 (Prior AUM \u00d7 NAV Return)  \u2022  "
    "Regular Plan Growth NAV used as MTM benchmark"
)
