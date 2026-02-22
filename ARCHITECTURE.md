# Architecture & Codebase Guide

This document explains how every file works so anyone can pick up the project.

---

## 1. pipeline.py — Data Pipeline

This is the core engine. It fetches data from AMFI, computes flows, and stores everything.

### How it works (step by step)

When you call `compute_flows_for_month(year, month)`:

1. **Determine dates**: Computes the last business day of the target month and
   the previous month (skipping weekends).
   - Example: `compute_flows_for_month(2026, 1)` → current = 30-Jan-2026, previous = 31-Dec-2025

2. **Fetch current month data**: Calls `fetch_all_schemes_for_date(date_str)` which
   iterates over all 12 equity + 7 hybrid sub-categories, calling the AMFI-CRISIL API
   for each. Returns a DataFrame with scheme_name, category, sub_category, NAV, AUM.
   - If the exact last business day has no data (holiday), it retries up to 4 days back.

3. **Fetch previous month data**: Same process for the prior month-end.

4. **Store snapshots**: Both months' raw data → `monthly_snapshots` table.
   Uses DELETE-then-INSERT to handle re-runs cleanly.

5. **Compute flows**: Merges current + previous on `scheme_name`, then:
   ```
   nav_return      = nav_cur / nav_prev
   expected_aum_cr = aum_prev_cr × nav_return
   net_flow_cr     = aum_cur_cr − expected_aum_cr
   flow_pct        = net_flow_cr / aum_prev_cr × 100
   ```

6. **Store flows**: Writes to `monthly_flows` table (DELETE-then-INSERT for idempotency).

7. **Log**: Records success/failure in `pipeline_log` table.

### API Details

The AMFI-CRISIL API is a set of POST endpoints:

```
Base URL: https://www.amfiindia.com/gateway/pollingsebi/api/amfi/

Endpoint: fundperformance
Payload:  { maturityType: 1, category: 1, subCategory: 3, mfid: 17, reportDate: "30-Jan-2026" }
Response: { validationMsg: "SUCCESS", data: [ { schemeName, navRegular, navDirect, dailyAUM, ... } ] }
```

- `maturityType=1` means Open Ended funds
- `category`: 1=Equity, 3=Hybrid
- `subCategory`: 1=Large Cap, 2=Large & Mid Cap, ..., 12=Sectoral/Thematic (for Equity);
  30-35, 40 for Hybrid
- `mfid`: AMC identifier (17 = ICICI Prudential)
- `reportDate`: format `dd-MMM-yyyy` (e.g. "30-Jan-2026")

### Key functions

| Function | Purpose |
|----------|---------|
| `init_db()` | Creates SQLite tables if they don't exist |
| `get_last_business_day(year, month)` | Returns last weekday of month as `dd-MMM-yyyy` |
| `fetch_schemes_for_date(date, cat, subcat, ...)` | Fetches schemes for one sub-category on one date |
| `fetch_all_schemes_for_date(date)` | Iterates all equity + hybrid sub-categories |
| `store_snapshot(df, month_end_iso)` | Writes raw snapshot to DB |
| `compute_flows_for_month(year, month)` | **Main entry point**: fetch + compute + store |
| `load_flows(months=36)` | Query helper: returns flow data for dashboard |
| `load_pipeline_log()` | Query helper: returns recent pipeline runs |
| `load_snapshots()` | Query helper: returns all snapshots |

### CLI usage

```bash
# Fetch single month
python pipeline.py --year 2026 --month 1

# Backfill 6 months ending at Jun 2025 (fetches Jan-Jun 2025)
python pipeline.py --year 2025 --month 6 --backfill 5

# Default (no args): processes previous month
python pipeline.py
```

---

## 2. app.py — Streamlit Dashboard

The interactive web dashboard. Runs on port 8501.

### Page layout

```
┌─────────────────────────────────────────────────────────────────────┐
│ SIDEBAR                    │  MAIN AREA                            │
│                            │                                       │
│ ▸ Fetch New Data           │  Title + Formula                      │
│   [Year] [Month]           │  [Month selector dropdown]            │
│   [Fetch & Compute]        │                                       │
│                            │  ┌─ KPI Cards ──────────────────────┐ │
│ ▸ Load Multiple Months     │  │ Net Flow │ AUM │ In │ Out │ F/A% │ │
│   [Months back] [Load]     │  └──────────────────────────────────┘ │
│                            │                                       │
│ ▸ Filters                  │  [Tab1] [Tab2] [Tab3] [Tab4]         │
│   [Equity] [Hybrid]        │  ┌──────────────────────────────────┐ │
│                            │  │  Chart content per tab           │ │
│ ▸ Formula reference        │  │  (see tab details below)         │ │
│                            │  └──────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### How data flows through the app

1. `load_data()` → calls `pipeline.load_flows(months=36)` → returns DataFrame from SQLite
2. Cached with `@st.cache_data(ttl=3600)` — refreshes hourly or when `st.cache_data.clear()` is called
3. Filtered by: category (sidebar), month (main dropdown)
4. `df_selected` = data for the chosen month → used by KPIs, Tab2, Tab4
5. Full `df` → used by Tab1 (time-series), Tab3 (heatmap)

### Sidebar actions

| Button | What it does |
|--------|-------------|
| **Fetch & Compute** | Calls `pipeline.compute_flows_for_month(year, month)` for the selected month, clears cache |
| **Load Historical Data** | Loops through N months backward from selected month, calls pipeline for each |

### Tabs breakdown

**Tab 1 — Monthly Trends** (all months, time-series):
- Chart 1: Net flow bars (green/red) + absolute labels + Flow/AUM % purple line
- Chart 2: AUM bars (blue) + absolute labels + YoY growth % amber line
- Chart 3: Sub-category stacked bar (top 10 categories by absolute flow)

**Tab 2 — Scheme Breakdown** (selected month):
- Top 10 inflow schemes (horizontal bar)
- Top 10 outflow schemes (horizontal bar)
- Top 20 schemes by AUM with net flow side-by-side
- Scatter plot: Flow vs AUM (size=AUM, color=direction)
- Drill-down: select any scheme → see its flow + AUM history across all months

**Tab 3 — Category Heatmap** (all months):
- Heatmap: sub-category rows × month columns, green=inflow, red=outflow
- Cumulative bar: total flow per sub-category across all loaded months

**Tab 4 — Raw Data** (selected month):
- Full data table with all computed columns
- CSV download button
- Pipeline run log

### Styling / theme

- Clean light theme (white backgrounds, dark text)
- Inter font family, base 16px
- Chart theme: `paper_bgcolor="#ffffff"`, `plot_bgcolor="#fafafa"`, font 13px
- Green (#16a34a) for inflows, Red (#dc2626) for outflows, Blue (#2563eb) for AUM
- All hover templates show zero decimals (`%{y:,.0f}`)

### Common patterns used

- `cliponaxis=False` + `margin=dict(r=100)` on horizontal bars to prevent label clipping
- `make_subplots(specs=[[{"secondary_y": True}]])` for dual-axis charts
- Manual `go.Bar` traces (not `px.bar`) for stacked bars to control hover order
- `traceorder="normal"` in legend to match visual stacking

---

## 3. scheduler.py — Auto-Update Scheduler

Minimal file. Uses APScheduler's `BlockingScheduler` to run on the 12th of every month.

```python
scheduler.add_job(monthly_job, "cron", day=12, hour=9, minute=30)
```

The job calls `pipeline.compute_flows_for_month()` for the previous month (since AMFI
publishes data around the 10th).

Run as: `python scheduler.py` (keeps running in foreground).

---

## 4. Database Schema (data/mf_flows.db)

SQLite database with 3 tables:

### monthly_snapshots
Raw month-end data from the API.
```sql
CREATE TABLE monthly_snapshots (
    scheme_name     TEXT,
    category        TEXT,       -- 'Equity' or 'Hybrid'
    sub_category    TEXT,       -- e.g. 'Large Cap', 'Flexi Cap'
    month_end       TEXT,       -- YYYY-MM-DD
    nav_regular     REAL,
    nav_direct      REAL,
    daily_aum_cr    REAL,       -- closing AUM in ₹ Cr
    PRIMARY KEY (scheme_name, month_end)
);
```

### monthly_flows
Computed flow data — this is what the dashboard reads.
```sql
CREATE TABLE monthly_flows (
    scheme_name     TEXT,
    category        TEXT,
    sub_category    TEXT,
    month_end       TEXT,       -- current month end (YYYY-MM-DD)
    prev_month_end  TEXT,
    nav_cur         REAL,
    nav_prev        REAL,
    nav_return      REAL,       -- nav_cur / nav_prev
    aum_cur_cr      REAL,
    aum_prev_cr     REAL,
    expected_aum_cr REAL,       -- aum_prev × nav_return
    net_flow_cr     REAL,       -- aum_cur − expected_aum
    flow_pct        REAL,       -- net_flow / aum_prev × 100
    PRIMARY KEY (scheme_name, month_end)
);
```

### pipeline_log
Audit trail of every pipeline run.
```sql
CREATE TABLE pipeline_log (
    run_at          TEXT,       -- ISO timestamp
    month_processed TEXT,       -- which month was processed
    schemes_updated INTEGER,
    status          TEXT,       -- 'SUCCESS' or 'FAILED'
    message         TEXT
);
```

---

## 5. requirements.txt

| Package | Purpose |
|---------|---------|
| `streamlit` | Dashboard web framework |
| `pandas` | Data manipulation |
| `numpy` | Numerical operations |
| `requests` | HTTP calls to AMFI API |
| `plotly` | Interactive charts (v6.x compatible) |
| `python-dateutil` | Date arithmetic (`relativedelta`) |
| `apscheduler` | Monthly cron-like scheduler |
| `sqlalchemy` | Optional DB helper (pandas uses it internally) |
| `openpyxl`, `xlrd` | Excel reading (legacy, not actively used) |

---

## Typical Development Workflow

1. **Add a new chart**: Edit `app.py`, add a new section inside the relevant `with tab:` block
2. **Change data fetching**: Edit `pipeline.py`, modify `fetch_schemes_for_date()` or add new endpoints
3. **Add a new AMC**: Change `ICICI_MF_ID` to another mfid, or parameterize it (see EXTENSION_PLAN.md)
4. **Test pipeline**: `python pipeline.py --year 2025 --month 6` then check `data/mf_flows.db`
5. **Restart dashboard**: Kill the Streamlit process and re-run `streamlit run app.py`
