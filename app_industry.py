"""
Industry-Level Mutual Fund Flow Dashboard (v2)
===============================================
Multi-AMC + sector/category analysis with time-series focus.
Period aggregation: Monthly | Quarterly | Financial Year | FY YTD.

Runs on port 8502.
Launch: streamlit run app_industry.py --server.port 8502
"""

import os, sqlite3, pandas as pd, numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date
from dateutil.relativedelta import relativedelta

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MF Industry Flows",
    page_icon="\U0001f30f",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys
sys.path.insert(0, os.path.dirname(__file__))
import pipeline_multi as pl
from amc_config import short_name, TOP_AMCS

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
    """Add fy, quarter, month_lbl columns to monthly data."""
    df = df.copy()
    df["fy"] = df["month_end"].apply(assign_fy)
    df["quarter"] = df["month_end"].apply(assign_quarter)
    df["month_lbl"] = df["month_end"].dt.strftime("%b '%y")
    return df


def agg_by_period(df, period, extra_group=None):
    """
    Aggregate monthly data to the selected period granularity.
    extra_group: additional columns to group by (e.g., ["amc"], ["sub_category"]).
    Returns: period_label, period_sort, net_flow_cr, aum_cr, flow_pct [+ extra cols].
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

    grp = extra_group + ["_period"]
    agg = df.groupby(grp, sort=False).agg(
        net_flow_cr=("net_flow_cr", "sum"),
        aum_cr=("aum_cur_cr", "last"),
        period_sort=("month_end", "max"),
    ).reset_index()
    agg.rename(columns={"_period": "period_label"}, inplace=True)
    agg["flow_pct"] = np.where(
        agg["aum_cr"] > 0, agg["net_flow_cr"] / agg["aum_cr"] * 100, 0
    )
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


def db_has_data():
    try:
        con = sqlite3.connect(DB_PATH)
        c = con.execute("SELECT COUNT(*) FROM industry_flows").fetchone()[0]
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
    st.markdown("## \U0001f30f Industry MF Flows")
    st.markdown("---")

    st.markdown("### \U0001f4e5 Fetch Data")
    st.caption("Fetches ALL AMCs at once (system-level) via mfid=0.")
    col_y, col_m = st.columns(2)
    with col_y:
        sel_year = st.selectbox("Year", list(range(2024, date.today().year + 1))[::-1])
    with col_m:
        sel_month = st.selectbox("Month", list(range(1, 13)),
                                  format_func=lambda m: pl.MONTH_ABBR[m])

    if st.button("\u2b07\ufe0f Fetch & Compute", use_container_width=True):
        with st.spinner(f"Fetching all AMCs for {pl.MONTH_ABBR[sel_month]} {sel_year}..."):
            try:
                pl.compute_flows_for_month(sel_year, sel_month)
                st.cache_data.clear()
                st.success("Done!")
            except Exception as e:
                st.error(f"Error: {e}")

    with st.expander("\U0001f4c5 Load Multiple Months"):
        hist_months = st.number_input("Months back", min_value=1, max_value=24, value=6)
        preview = []
        for i in range(hist_months, -1, -1):
            dt = date(sel_year, sel_month, 1) - relativedelta(months=i)
            preview.append(f"{pl.MONTH_ABBR[dt.month]} {dt.year}")
        st.info(f"**{preview[0]}** to **{preview[-1]}** ({len(preview)} months)")

        if st.button("\U0001f504 Load All", use_container_width=True):
            import time as _time
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
                    st.warning(f"Error: {pl.MONTH_ABBR[mn]} {yr}: {e}")
                progress.progress((idx + 1) / len(months_list))
                _time.sleep(0.5)
            st.cache_data.clear()
            st.success(f"Loaded {len(months_list)} months!")

    st.markdown("---")
    st.markdown("### \U0001f50d Filters")
    category_filter = st.multiselect("Category", ["Equity", "Hybrid"],
                                      default=["Equity", "Hybrid"])

    st.markdown("---")
    st.caption(
        "**Data Source**: AMFI-CRISIL API (mfid=0 = all AMCs)\n\n"
        "**Formula**: Flow = AUM(t) \u2212 AUM(t\u22121) \u00d7 NAV(t)/NAV(t\u22121)"
    )


# ── Main ─────────────────────────────────────────────────────────────────────
st.markdown("# \U0001f30f Mutual Fund Industry \u2014 Flow Dashboard")
st.markdown("*System-level analysis across all AMCs \u2014 Equity & Hybrid schemes*")

if not db_has_data():
    st.info("### No data yet\nUse the sidebar to fetch data.")
    st.stop()

df_all = load_data()
if df_all.empty:
    st.warning("No data available.")
    st.stop()

df_all = df_all[df_all["category"].isin(category_filter)]
if df_all.empty:
    st.warning("No data matches filters.")
    st.stop()


# ── Period Selector ──────────────────────────────────────────────────────────
period = st.radio(
    "View Period",
    ["Monthly", "Quarterly", "Financial Year", "FY YTD"],
    horizontal=True,
    index=0,
)

# Latest month for reference
latest_month = df_all["month_end"].max()
latest_month_lbl = pd.Timestamp(latest_month).strftime("%B %Y")

# FY YTD cumulative
cfy = get_current_fy()
df_fy_ytd = df_all[df_all["fy"] == cfy]
fy_ytd_flow = df_fy_ytd["net_flow_cr"].sum()
fy_ytd_months = df_fy_ytd["month_end"].nunique()

# Latest month aggregates
df_latest = df_all[df_all["month_end"] == latest_month]
total_flow_latest = df_latest["net_flow_cr"].sum()
total_aum_latest = df_latest["aum_cur_cr"].sum()
n_schemes = df_latest["scheme_name"].nunique()
n_amcs = df_latest["amc"].nunique()
flow_pct_latest = (total_flow_latest / total_aum_latest * 100) if total_aum_latest else 0


# ── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown(
    f"<div class='section-header'>\U0001f4cc Industry Snapshot \u2014 "
    f"{latest_month_lbl}</div>",
    unsafe_allow_html=True,
)

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    cls = "kpi-pos" if total_flow_latest >= 0 else "kpi-neg"
    st.markdown(kpi("Net Flow (Latest Month)", fmt_cr(total_flow_latest), latest_month_lbl, cls),
                unsafe_allow_html=True)
with k2:
    cls = "kpi-pos" if fy_ytd_flow >= 0 else "kpi-neg"
    st.markdown(kpi(f"{cfy} YTD Net Flow", fmt_cr(fy_ytd_flow),
                     f"{fy_ytd_months} months", cls),
                unsafe_allow_html=True)
with k3:
    st.markdown(kpi("Industry AUM", fmt_cr(total_aum_latest),
                     f"{n_schemes} schemes \u2022 {n_amcs} AMCs"),
                unsafe_allow_html=True)
with k4:
    inflow_amcs = df_latest.groupby("amc")["net_flow_cr"].sum()
    st.markdown(kpi("AMCs with Inflow", str((inflow_amcs > 0).sum()),
                     "positive net flow", "kpi-pos"),
                unsafe_allow_html=True)
with k5:
    cls = "kpi-pos" if flow_pct_latest >= 0 else "kpi-neg"
    st.markdown(kpi("Flow / AUM", f"{flow_pct_latest:+.2f}%",
                     "latest month", cls),
                unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ── Tabs ─────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "\U0001f4ca Industry Overview",
    "\U0001f3e2 AMC Market Share",
    "\U0001f50d AMC Deep-Dive & Scheme Type",
    "\U0001f4cb Raw Data",
])


# ═══════════════════════════════════════════════════════════════════════
# TAB 1 — INDUSTRY OVERVIEW (Time-Series First)
# ═══════════════════════════════════════════════════════════════════════
with tab1:
    # Aggregate to selected period
    ind_agg = agg_by_period(df_all, period)

    if ind_agg.empty:
        st.warning("No data for selected period.")
    else:
        # ── Chart 1: Net Flow bars + Flow/AUM % line ──────────
        st.markdown(
            "<div class='section-header'>Net Inflows / Outflows</div>",
            unsafe_allow_html=True,
        )

        fig_flow = make_subplots(specs=[[{"secondary_y": True}]])
        flow_colors = [COLOR_POS if v >= 0 else COLOR_NEG for v in ind_agg["net_flow_cr"]]
        flow_labels = [fmt_cr(v) for v in ind_agg["net_flow_cr"]]

        fig_flow.add_trace(go.Bar(
            x=ind_agg["period_label"], y=ind_agg["net_flow_cr"],
            name="Net Flow", marker_color=flow_colors, opacity=0.85,
            text=flow_labels, textposition="outside", textfont=dict(size=11),
            hovertemplate="Net Flow: \u20b9%{y:,.0f} Cr<extra></extra>",
        ), secondary_y=False)

        fig_flow.add_trace(go.Scatter(
            x=ind_agg["period_label"], y=ind_agg["flow_pct"],
            name="Flow / AUM %", line=dict(color="#7c3aed", width=2.5),
            mode="lines+markers", marker=dict(size=6),
            hovertemplate="Flow/AUM: %{y:+.2f}%<extra></extra>",
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

        # ── Chart 2: AUM bars + YoY Growth % ──────────────────
        st.markdown(
            "<div class='section-header'>Total AUM & YoY Growth</div>",
            unsafe_allow_html=True,
        )

        aum_agg = ind_agg.copy()
        aum_labels = [fmt_cr(v) for v in aum_agg["aum_cr"]]

        # Compute YoY: compare to period 12 months prior (approximate)
        aum_agg = aum_agg.reset_index(drop=True)
        yoy = []
        for i, row in aum_agg.iterrows():
            target = row["period_sort"] - pd.DateOffset(years=1)
            prev = aum_agg[aum_agg["period_sort"].between(
                target - pd.Timedelta(days=45), target + pd.Timedelta(days=45)
            )]
            if not prev.empty and prev.iloc[0]["aum_cr"] > 0:
                yoy.append((row["aum_cr"] - prev.iloc[0]["aum_cr"]) / prev.iloc[0]["aum_cr"] * 100)
            else:
                yoy.append(None)
        aum_agg["yoy_pct"] = yoy

        fig_aum = make_subplots(specs=[[{"secondary_y": True}]])
        fig_aum.add_trace(go.Bar(
            x=aum_agg["period_label"], y=aum_agg["aum_cr"],
            name="AUM", marker_color=COLOR_AUM, opacity=0.85,
            text=aum_labels, textposition="outside", textfont=dict(size=11),
            hovertemplate="AUM: \u20b9%{y:,.0f} Cr<extra></extra>",
        ), secondary_y=False)

        fig_aum.add_trace(go.Scatter(
            x=aum_agg["period_label"], y=aum_agg["yoy_pct"],
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

    # ── Category stacked bar time-series ──────────────────────
    st.markdown(
        "<div class='section-header'>Category Net Flow Breakdown</div>",
        unsafe_allow_html=True,
    )

    cat_agg = agg_by_period(df_all, period, extra_group=["sub_category"])
    if not cat_agg.empty:
        cat_pivot = cat_agg.pivot_table(
            index="period_label", columns="sub_category",
            values="net_flow_cr", aggfunc="sum"
        ).fillna(0)
        # Sort by period
        order_map = cat_agg.drop_duplicates("period_label").set_index("period_label")["period_sort"]
        sorted_idx = order_map.sort_values().index.tolist()
        cat_pivot = cat_pivot.reindex([x for x in sorted_idx if x in cat_pivot.index])

        palette = px.colors.qualitative.Set2 + px.colors.qualitative.Set3
        fig_cat_ts = go.Figure()
        for i, col in enumerate(cat_pivot.columns):
            fig_cat_ts.add_trace(go.Bar(
                x=cat_pivot.index, y=cat_pivot[col], name=col,
                marker_color=palette[i % len(palette)],
                hovertemplate=f"<b>{col}</b>: " + "\u20b9%{y:,.0f} Cr<extra></extra>",
            ))
        fig_cat_ts.update_layout(
            barmode="relative", height=480,
            **CHART_THEME, hovermode="x",
            legend=dict(orientation="h", y=-0.3, font=dict(size=10), traceorder="normal"),
            xaxis=dict(tickfont=dict(size=11), **AXIS_STYLE),
            yaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
        )
        st.plotly_chart(fig_cat_ts, use_container_width=True)

    # ── Category Heatmap ──────────────────────────────────────
    st.markdown(
        "<div class='section-header'>Flow Heatmap \u2014 Category \u00d7 Period</div>",
        unsafe_allow_html=True,
    )

    if not cat_agg.empty:
        hm_pivot = cat_agg.pivot_table(
            index="sub_category", columns="period_label",
            values="net_flow_cr", aggfunc="sum"
        ).fillna(0)
        hm_pivot = hm_pivot.reindex(
            columns=[x for x in sorted_idx if x in hm_pivot.columns]
        )
        if not hm_pivot.empty:
            fig_hm = go.Figure(data=go.Heatmap(
                z=hm_pivot.values, x=hm_pivot.columns.tolist(),
                y=hm_pivot.index.tolist(),
                colorscale=[[0, "#dc2626"], [0.4, "#fecaca"],
                            [0.5, "#f3f4f6"], [0.6, "#bbf7d0"], [1, "#16a34a"]],
                zmid=0,
                text=[[fmt_cr(v) for v in row] for row in hm_pivot.values],
                texttemplate="%{text}", textfont=dict(size=9, color="#1f2937"),
                hovertemplate="<b>%{y}</b><br>%{x}: %{text}<extra></extra>",
                colorbar=dict(title="\u20b9 Cr", tickfont=dict(size=10)),
            ))
            fig_hm.update_layout(
                height=max(350, len(hm_pivot) * 40),
                **CHART_THEME,
                xaxis=dict(side="top", tickfont=dict(size=11)),
                yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
            )
            st.plotly_chart(fig_hm, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 2 — AMC MARKET SHARE & COMPARISON
# ═══════════════════════════════════════════════════════════════════════
with tab2:
    # Scheme type filter for this tab
    all_subcats = sorted(df_all["sub_category"].unique())
    scheme_type_filter = st.selectbox(
        "Filter by Scheme Type",
        ["All Scheme Types"] + all_subcats,
        key="tab2_scheme_type",
    )
    df_tab2 = df_all if scheme_type_filter == "All Scheme Types" else \
              df_all[df_all["sub_category"] == scheme_type_filter]

    if df_tab2.empty:
        st.warning("No data for selected scheme type.")
    else:
        # Aggregate by AMC for selected period
        amc_agg = agg_by_period(df_tab2, period, extra_group=["amc"])

        # Latest period
        latest_period_sort = amc_agg["period_sort"].max()
        latest_period_lbl = amc_agg.loc[
            amc_agg["period_sort"] == latest_period_sort, "period_label"
        ].iloc[0]
        amc_latest = amc_agg[amc_agg["period_sort"] == latest_period_sort].copy()

        total_flow_period = amc_latest["net_flow_cr"].sum()
        total_aum_period = amc_latest["aum_cr"].sum()

        # Compute market shares
        amc_latest["flow_share"] = np.where(
            total_flow_period != 0,
            amc_latest["net_flow_cr"] / total_flow_period * 100, 0
        )
        amc_latest["aum_share"] = np.where(
            total_aum_period > 0,
            amc_latest["aum_cr"] / total_aum_period * 100, 0
        )

        # ── AMC Net Inflow Ranking ──────────────────────────────
        st.markdown(
            f"<div class='section-header'>AMC Net Inflow Ranking \u2014 "
            f"{latest_period_lbl}</div>",
            unsafe_allow_html=True,
        )

        amc_sorted = amc_latest.sort_values("net_flow_cr")
        top_amc = pd.concat([
            amc_sorted.nlargest(15, "net_flow_cr"),
            amc_sorted.nsmallest(5, "net_flow_cr")
        ]).drop_duplicates().sort_values("net_flow_cr")
        amc_colors = [COLOR_POS if v >= 0 else COLOR_NEG for v in top_amc["net_flow_cr"]]

        fig_amc_flow = go.Figure(go.Bar(
            x=top_amc["net_flow_cr"], y=top_amc["amc"],
            orientation="h", marker_color=amc_colors,
            text=[fmt_cr(v) for v in top_amc["net_flow_cr"]],
            textposition="outside", textfont=dict(size=11), cliponaxis=False,
            hovertemplate="<b>%{y}</b><br>Flow: \u20b9%{x:,.0f} Cr<extra></extra>",
        ))
        fig_amc_flow.update_layout(
            height=max(500, len(top_amc) * 28),
            **CHART_THEME, showlegend=False,
            margin=dict(r=120, l=10),
            xaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
            yaxis=dict(tickfont=dict(size=11), **AXIS_STYLE),
        )
        st.plotly_chart(fig_amc_flow, use_container_width=True)

        # ── AMC AUM Ranking ──────────────────────────────────────
        st.markdown(
            f"<div class='section-header'>AMC Ranking by AUM \u2014 "
            f"{latest_period_lbl}</div>",
            unsafe_allow_html=True,
        )

        amc_by_aum = amc_latest.nlargest(20, "aum_cr").sort_values("aum_cr")
        fig_aum_rank = make_subplots(
            rows=1, cols=2, shared_yaxes=True,
            column_widths=[0.6, 0.4], horizontal_spacing=0.02,
            subplot_titles=("AUM (\u20b9 Cr)", "Net Flow (\u20b9 Cr)"),
        )
        fig_aum_rank.add_trace(go.Bar(
            x=amc_by_aum["aum_cr"], y=amc_by_aum["amc"],
            orientation="h", marker_color=COLOR_AUM, opacity=0.8,
            text=[fmt_cr(v) for v in amc_by_aum["aum_cr"]],
            textposition="outside", textfont=dict(size=10), cliponaxis=False,
            name="AUM",
            hovertemplate="<b>%{y}</b><br>AUM: \u20b9%{x:,.0f} Cr<extra></extra>",
        ), row=1, col=1)

        fc2 = [COLOR_POS if v >= 0 else COLOR_NEG for v in amc_by_aum["net_flow_cr"]]
        fig_aum_rank.add_trace(go.Bar(
            x=amc_by_aum["net_flow_cr"], y=amc_by_aum["amc"],
            orientation="h", marker_color=fc2,
            text=[fmt_cr(v) for v in amc_by_aum["net_flow_cr"]],
            textposition="outside", textfont=dict(size=10), cliponaxis=False,
            name="Flow",
            hovertemplate="<b>%{y}</b><br>Flow: \u20b9%{x:,.0f} Cr<extra></extra>",
        ), row=1, col=2)

        fig_aum_rank.update_layout(
            height=max(500, len(amc_by_aum) * 28),
            **CHART_THEME, showlegend=False, margin=dict(r=100, l=10),
        )
        fig_aum_rank.update_xaxes(**AXIS_STYLE)
        fig_aum_rank.update_yaxes(tickfont=dict(size=11), **AXIS_STYLE)
        st.plotly_chart(fig_aum_rank, use_container_width=True)

        # ── Market Share Comparison: AUM share vs Flow share (scatter) ──
        st.markdown(
            f"<div class='section-header'>AUM Market Share vs Net Inflow Share \u2014 "
            f"{latest_period_lbl}</div>",
            unsafe_allow_html=True,
        )
        st.caption(
            "AMCs **above** the diagonal are gaining market share "
            "(flow share > AUM share). AMCs **below** are losing share."
        )

        scatter_df = amc_latest[amc_latest["aum_share"] > 0.3].copy()  # show only meaningful
        scatter_df["size"] = scatter_df["aum_cr"].clip(lower=100)

        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(
            x=scatter_df["aum_share"], y=scatter_df["flow_share"],
            mode="markers+text", text=scatter_df["amc"],
            textposition="top center", textfont=dict(size=9),
            marker=dict(
                size=scatter_df["aum_share"] * 3 + 8,
                color=[COLOR_POS if v >= 0 else COLOR_NEG for v in scatter_df["flow_share"]],
                opacity=0.7, line=dict(width=1, color="#d1d5db"),
            ),
            hovertemplate=(
                "<b>%{text}</b><br>"
                "AUM Share: %{x:.1f}%<br>"
                "Flow Share: %{y:.1f}%<extra></extra>"
            ),
        ))
        # Diagonal line
        max_val = max(scatter_df["aum_share"].max(), abs(scatter_df["flow_share"]).max(), 10) * 1.1
        fig_scatter.add_shape(
            type="line", x0=0, y0=0, x1=max_val, y1=max_val,
            line=dict(color="#9ca3af", width=1, dash="dash"),
        )
        fig_scatter.update_layout(
            height=520, **CHART_THEME,
            xaxis=dict(title="AUM Market Share (%)", **AXIS_STYLE),
            yaxis=dict(title="Net Inflow Share (%)", zeroline=True,
                       zerolinecolor="#d1d5db", **AXIS_STYLE),
            showlegend=False,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        # ── AMC Flow Time-Series (top 10) ─────────────────────────
        st.markdown(
            "<div class='section-header'>AMC Net Flow Trends (Top 10)</div>",
            unsafe_allow_html=True,
        )

        if len(amc_agg["period_label"].unique()) > 1:
            top_amcs = (
                amc_agg.groupby("amc")["net_flow_cr"]
                .apply(lambda x: x.abs().sum())
                .nlargest(10).index.tolist()
            )
            amc_ts = amc_agg[amc_agg["amc"].isin(top_amcs)].sort_values("period_sort")

            fig_amc_trend = px.line(
                amc_ts, x="period_label", y="net_flow_cr", color="amc",
                markers=True, height=460,
                labels={"net_flow_cr": "Net Flow (\u20b9 Cr)",
                        "period_label": "", "amc": "AMC"},
            )
            fig_amc_trend.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Flow: \u20b9%{y:,.0f} Cr<extra></extra>"
            )
            fig_amc_trend.add_hline(y=0, line_dash="dash", line_color="#9ca3af", line_width=1)
            fig_amc_trend.update_layout(
                **CHART_THEME, hovermode="x",
                xaxis=dict(**AXIS_STYLE), yaxis=dict(**AXIS_STYLE),
                legend=dict(font=dict(size=11)),
            )
            st.plotly_chart(fig_amc_trend, use_container_width=True)

        # ── AMC Market Share Time-Series ──────────────────────────
        if len(amc_agg["period_label"].unique()) > 1:
            st.markdown(
                "<div class='section-header'>AUM Market Share Trends (Top 10 AMCs)</div>",
                unsafe_allow_html=True,
            )
            # Compute AUM share by period
            period_totals = amc_agg.groupby("period_label").agg(
                total_aum=("aum_cr", "sum"),
            ).reset_index()
            amc_share_ts = amc_agg.merge(period_totals, on="period_label")
            amc_share_ts["aum_share"] = amc_share_ts["aum_cr"] / amc_share_ts["total_aum"] * 100

            top_aum_amcs = (
                amc_share_ts.groupby("amc")["aum_cr"].sum()
                .nlargest(10).index.tolist()
            )
            share_ts_top = amc_share_ts[amc_share_ts["amc"].isin(top_aum_amcs)].sort_values("period_sort")

            fig_share_trend = px.line(
                share_ts_top, x="period_label", y="aum_share", color="amc",
                markers=True, height=460,
                labels={"aum_share": "AUM Share (%)", "period_label": "", "amc": "AMC"},
            )
            fig_share_trend.update_traces(
                hovertemplate="<b>%{fullData.name}</b><br>Share: %{y:.1f}%<extra></extra>"
            )
            fig_share_trend.update_layout(
                **CHART_THEME, hovermode="x",
                xaxis=dict(**AXIS_STYLE), yaxis=dict(title="AUM Share (%)", **AXIS_STYLE),
                legend=dict(font=dict(size=11)),
            )
            st.plotly_chart(fig_share_trend, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 3 — AMC DEEP-DIVE & SCHEME TYPE
# ═══════════════════════════════════════════════════════════════════════
with tab3:
    col_amc, col_sch = st.columns(2)
    with col_amc:
        all_amcs = sorted(df_all["amc"].unique())
        sel_amc = st.selectbox("Select AMC", all_amcs,
                                index=all_amcs.index("ICICI Pru") if "ICICI Pru" in all_amcs else 0)
    with col_sch:
        all_subcats_dd = sorted(df_all["sub_category"].unique())
        sel_subcat = st.selectbox("Select Scheme Type",
                                   ["All Scheme Types"] + all_subcats_dd,
                                   key="tab3_scheme_type")

    # Filter data
    df_amc = df_all[df_all["amc"] == sel_amc]
    if sel_subcat != "All Scheme Types":
        df_amc = df_amc[df_amc["sub_category"] == sel_subcat]

    if df_amc.empty:
        st.warning(f"No data for {sel_amc} / {sel_subcat}.")
    else:
        # ── AMC KPIs ──────────────────────────────────────────────
        df_amc_latest = df_amc[df_amc["month_end"] == latest_month]
        amc_flow = df_amc_latest["net_flow_cr"].sum()
        amc_aum = df_amc_latest["aum_cur_cr"].sum()
        amc_n = df_amc_latest["scheme_name"].nunique()
        amc_flow_pct = (amc_flow / amc_aum * 100) if amc_aum else 0

        # FY YTD for this AMC
        df_amc_fy = df_amc[df_amc["fy"] == cfy]
        amc_fy_flow = df_amc_fy["net_flow_cr"].sum()

        subcat_lbl = f" \u2014 {sel_subcat}" if sel_subcat != "All Scheme Types" else ""
        st.markdown(
            f"<div class='section-header'>{sel_amc}{subcat_lbl} \u2014 "
            f"{latest_month_lbl}</div>",
            unsafe_allow_html=True,
        )

        ak1, ak2, ak3, ak4 = st.columns(4)
        with ak1:
            cls = "kpi-pos" if amc_flow >= 0 else "kpi-neg"
            st.markdown(kpi("Net Flow", fmt_cr(amc_flow), latest_month_lbl, cls),
                        unsafe_allow_html=True)
        with ak2:
            cls = "kpi-pos" if amc_fy_flow >= 0 else "kpi-neg"
            st.markdown(kpi(f"{cfy} YTD Flow", fmt_cr(amc_fy_flow), "", cls),
                        unsafe_allow_html=True)
        with ak3:
            st.markdown(kpi("AUM", fmt_cr(amc_aum), f"{amc_n} schemes"),
                        unsafe_allow_html=True)
        with ak4:
            cls = "kpi-pos" if amc_flow_pct >= 0 else "kpi-neg"
            st.markdown(kpi("Flow/AUM", f"{amc_flow_pct:+.2f}%", "", cls),
                        unsafe_allow_html=True)

        # ── AMC Flow Time-Series ──────────────────────────────────
        st.markdown(
            "<div class='section-header'>Net Flow Time-Series</div>",
            unsafe_allow_html=True,
        )

        amc_period = agg_by_period(df_amc, period)
        if not amc_period.empty and len(amc_period) > 0:
            fig_amc_ts = make_subplots(specs=[[{"secondary_y": True}]])
            fc = [COLOR_POS if v >= 0 else COLOR_NEG for v in amc_period["net_flow_cr"]]
            fig_amc_ts.add_trace(go.Bar(
                x=amc_period["period_label"], y=amc_period["net_flow_cr"],
                marker_color=fc, opacity=0.85, name="Net Flow",
                text=[fmt_cr(v) for v in amc_period["net_flow_cr"]],
                textposition="outside", textfont=dict(size=10),
                hovertemplate="Flow: \u20b9%{y:,.0f} Cr<extra></extra>",
            ), secondary_y=False)
            fig_amc_ts.add_trace(go.Scatter(
                x=amc_period["period_label"], y=amc_period["flow_pct"],
                name="Flow/AUM %", line=dict(color="#7c3aed", width=2.5),
                mode="lines+markers", marker=dict(size=5),
                hovertemplate="Flow/AUM: %{y:+.2f}%<extra></extra>",
            ), secondary_y=True)
            fig_amc_ts.update_layout(
                height=420, **CHART_THEME, hovermode="x",
                legend=dict(orientation="h", y=1.08, font=dict(size=11)),
                xaxis=dict(**AXIS_STYLE, tickfont=dict(size=11)),
                yaxis=dict(title="Net Flow (\u20b9 Cr)", zeroline=True,
                           zerolinecolor="#d1d5db", **AXIS_STYLE),
                yaxis2=dict(title="Flow/AUM %", gridcolor="rgba(0,0,0,0)",
                            ticksuffix="%"),
            )
            st.plotly_chart(fig_amc_ts, use_container_width=True)

        # ── Which AMCs doing well in this scheme type? ────────────
        if sel_subcat != "All Scheme Types":
            st.markdown(
                f"<div class='section-header'>AMC Comparison \u2014 "
                f"{sel_subcat}</div>",
                unsafe_allow_html=True,
            )
            df_subcat = df_all[df_all["sub_category"] == sel_subcat]
            subcat_amc_agg = agg_by_period(df_subcat, period, extra_group=["amc"])

            if not subcat_amc_agg.empty:
                # Latest period ranking
                sp_latest = subcat_amc_agg[
                    subcat_amc_agg["period_sort"] == subcat_amc_agg["period_sort"].max()
                ].sort_values("net_flow_cr")

                fig_subcat_rank = go.Figure(go.Bar(
                    x=sp_latest["net_flow_cr"], y=sp_latest["amc"],
                    orientation="h",
                    marker_color=[COLOR_POS if v >= 0 else COLOR_NEG
                                  for v in sp_latest["net_flow_cr"]],
                    text=[fmt_cr(v) for v in sp_latest["net_flow_cr"]],
                    textposition="outside", textfont=dict(size=10), cliponaxis=False,
                    hovertemplate="<b>%{y}</b>: \u20b9%{x:,.0f} Cr<extra></extra>",
                ))
                fig_subcat_rank.update_layout(
                    height=max(400, len(sp_latest) * 25),
                    **CHART_THEME, showlegend=False, margin=dict(r=100),
                    xaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
                    yaxis=dict(tickfont=dict(size=10), **AXIS_STYLE),
                )
                st.plotly_chart(fig_subcat_rank, use_container_width=True)

                # Time-series for top AMCs in this sub-category
                if len(subcat_amc_agg["period_label"].unique()) > 1:
                    top_in_cat = (
                        subcat_amc_agg.groupby("amc")["net_flow_cr"]
                        .apply(lambda x: x.abs().sum())
                        .nlargest(8).index.tolist()
                    )
                    cat_ts = subcat_amc_agg[
                        subcat_amc_agg["amc"].isin(top_in_cat)
                    ].sort_values("period_sort")

                    fig_cat_amc_ts = px.line(
                        cat_ts, x="period_label", y="net_flow_cr", color="amc",
                        markers=True, height=420,
                        labels={"net_flow_cr": "Net Flow (\u20b9 Cr)",
                                "period_label": "", "amc": "AMC"},
                    )
                    fig_cat_amc_ts.update_traces(
                        hovertemplate="<b>%{fullData.name}</b><br>\u20b9%{y:,.0f} Cr<extra></extra>"
                    )
                    fig_cat_amc_ts.add_hline(y=0, line_dash="dash",
                                              line_color="#9ca3af", line_width=1)
                    fig_cat_amc_ts.update_layout(
                        **CHART_THEME, hovermode="x",
                        xaxis=dict(**AXIS_STYLE),
                        yaxis=dict(**AXIS_STYLE),
                        legend=dict(font=dict(size=10)),
                    )
                    st.plotly_chart(fig_cat_amc_ts, use_container_width=True)

        # ── Category Breakdown (if showing all scheme types) ──────
        if sel_subcat == "All Scheme Types" and not df_amc_latest.empty:
            st.markdown(
                "<div class='section-header'>Category Breakdown</div>",
                unsafe_allow_html=True,
            )
            amc_cat = (
                df_amc_latest.groupby("sub_category")
                .agg(net_flow=("net_flow_cr", "sum"), aum=("aum_cur_cr", "sum"))
                .reset_index().sort_values("net_flow")
            )
            fig_ac = go.Figure(go.Bar(
                x=amc_cat["net_flow"], y=amc_cat["sub_category"],
                orientation="h",
                marker_color=[COLOR_POS if v >= 0 else COLOR_NEG for v in amc_cat["net_flow"]],
                text=[fmt_cr(v) for v in amc_cat["net_flow"]],
                textposition="outside", textfont=dict(size=11), cliponaxis=False,
                hovertemplate="<b>%{y}</b>: \u20b9%{x:,.0f} Cr<extra></extra>",
            ))
            fig_ac.update_layout(
                height=max(350, len(amc_cat) * 30),
                **CHART_THEME, showlegend=False, margin=dict(r=100),
                xaxis=dict(title="Net Flow (\u20b9 Cr)", **AXIS_STYLE),
                yaxis=dict(tickfont=dict(size=11), **AXIS_STYLE),
            )
            st.plotly_chart(fig_ac, use_container_width=True)

        # ── Top Schemes ───────────────────────────────────────────
        if not df_amc_latest.empty:
            col_l, col_r = st.columns(2)
            with col_l:
                st.markdown("**\U0001f7e2 Top Inflow Schemes**")
                top_in = df_amc_latest.nlargest(10, "net_flow_cr")[
                    ["scheme_name", "net_flow_cr", "aum_cur_cr"]
                ].copy()
                top_in["short"] = top_in["scheme_name"].apply(
                    lambda s: s.split(" ", 2)[-1] if " " in s else s
                )
                sorted_in = top_in.sort_values("net_flow_cr")
                fig_in = go.Figure(go.Bar(
                    x=sorted_in["net_flow_cr"], y=sorted_in["short"],
                    orientation="h", marker_color=COLOR_POS,
                    text=[fmt_cr(v) for v in sorted_in["net_flow_cr"]],
                    textposition="outside", textfont=dict(size=10), cliponaxis=False,
                    hovertemplate="<b>%{y}</b>: \u20b9%{x:,.0f} Cr<extra></extra>",
                ))
                fig_in.update_layout(
                    height=380, **CHART_THEME, showlegend=False,
                    margin=dict(r=80),
                    xaxis=dict(**AXIS_STYLE),
                    yaxis=dict(tickfont=dict(size=10), **AXIS_STYLE),
                )
                st.plotly_chart(fig_in, use_container_width=True)

            with col_r:
                st.markdown("**\U0001f534 Top Outflow Schemes**")
                top_out = df_amc_latest.nsmallest(10, "net_flow_cr")[
                    ["scheme_name", "net_flow_cr", "aum_cur_cr"]
                ].copy()
                top_out["short"] = top_out["scheme_name"].apply(
                    lambda s: s.split(" ", 2)[-1] if " " in s else s
                )
                sorted_out = top_out.sort_values("net_flow_cr", ascending=False)
                fig_out = go.Figure(go.Bar(
                    x=sorted_out["net_flow_cr"], y=sorted_out["short"],
                    orientation="h", marker_color=COLOR_NEG,
                    text=[fmt_cr(v) for v in sorted_out["net_flow_cr"]],
                    textposition="outside", textfont=dict(size=10), cliponaxis=False,
                    hovertemplate="<b>%{y}</b>: \u20b9%{x:,.0f} Cr<extra></extra>",
                ))
                fig_out.update_layout(
                    height=380, **CHART_THEME, showlegend=False,
                    margin=dict(l=80),
                    xaxis=dict(**AXIS_STYLE),
                    yaxis=dict(tickfont=dict(size=10), **AXIS_STYLE),
                )
                st.plotly_chart(fig_out, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════
# TAB 4 — RAW DATA
# ═══════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown(
        "<div class='section-header'>Full Data \u2014 All Months Loaded</div>",
        unsafe_allow_html=True,
    )

    show_df = df_all[[
        "month_lbl", "amc", "scheme_name", "category", "sub_category",
        "aum_cur_cr", "aum_prev_cr", "net_flow_cr", "flow_pct",
    ]].copy().sort_values(["month_end", "net_flow_cr"], ascending=[False, False])

    show_df.columns = [
        "Month", "AMC", "Scheme", "Category", "Sub-Category",
        "AUM Cur (\u20b9Cr)", "AUM Prev (\u20b9Cr)", "Net Flow (\u20b9Cr)", "Flow %",
    ]
    for col in ["AUM Cur (\u20b9Cr)", "AUM Prev (\u20b9Cr)", "Net Flow (\u20b9Cr)"]:
        show_df[col] = show_df[col].apply(lambda x: round(x, 0) if pd.notna(x) else None)
    show_df["Flow %"] = show_df["Flow %"].apply(
        lambda x: f"{x:+.2f}%" if pd.notna(x) else None
    )

    st.dataframe(show_df, use_container_width=True, height=600)

    csv = show_df.to_csv(index=False)
    st.download_button("\u2b07\ufe0f Download CSV", csv, "industry_flows.csv", "text/csv")


# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data: AMFI-CRISIL Fund Performance API (mfid=0)  \u2022  "
    "Flows = Actual AUM \u2212 (Prior AUM \u00d7 NAV Return)  \u2022  "
    "Regular Plan Growth NAV used as MTM benchmark"
)
