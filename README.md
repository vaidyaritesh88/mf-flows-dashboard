# Mutual Fund Flow Dashboard — ICICI Prudential AMC

Computes and visualises **net monthly flows** for ICICI Prudential equity & hybrid
mutual fund schemes using the AMFI-CRISIL Fund Performance API.

## Flow Formula

```
Expected_AUM(t) = AUM(t-1)  ×  [ NAV(t) / NAV(t-1) ]
Net_Flow(t)     = AUM(t)    −  Expected_AUM(t)
```

- **AUM** = Month-end closing AUM from AMFI (₹ Cr)
- **NAV** = Regular Plan Growth NAV from AMFI
- Growth NAV is used as MTM benchmark for ALL plan options (including IDCW/Dividend variants)

## Data Source

All data comes from the **AMFI-CRISIL Fund Performance API** — the same API that
powers the AMFI website's fund performance iframe. No API key required.

| Endpoint | Purpose |
|----------|---------|
| `fundperformancefilters` | Get list of AMCs and filter options |
| `getsubcategory` | Get sub-categories for a given category |
| `fundperformance` | Get scheme-level AUM, NAV, returns for a date |

**Base URL**: `https://www.amfiindia.com/gateway/pollingsebi/api/amfi/`

## Quick Start

```bash
# 1. Create virtual environment
python -m venv venv
venv\Scripts\activate            # Linux/Mac: source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Fetch data for a specific month
python pipeline.py --year 2026 --month 1

# 4. Backfill several months at once (e.g. 6 months ending Jun 2025)
python pipeline.py --year 2025 --month 6 --backfill 5

# 5. Launch dashboard
streamlit run app.py --server.port 8501
```

Dashboard opens at `http://localhost:8501`.

## Project Structure

```
Flows dashboard/
├── app.py              # Streamlit dashboard (port 8501)
├── pipeline.py         # Data pipeline: API fetch + flow computation + SQLite storage
├── scheduler.py        # APScheduler: auto-runs on 12th of each month at 09:30 IST
├── requirements.txt    # Python dependencies
├── README.md           # This file
├── ARCHITECTURE.md     # Detailed technical architecture doc
├── EXTENSION_PLAN.md   # Plan for multi-AMC & sector-level analysis
└── data/
    └── mf_flows.db     # SQLite database (auto-created on first run)
```

## Dashboard Tabs

| Tab | What it shows |
|-----|---------------|
| **Monthly Trends** | Net flow bars with Flow/AUM % line, AUM bars with YoY growth line, sub-category stacked bar |
| **Scheme Breakdown** | Top 10 inflows, Top 10 outflows, Top 20 by AUM with flows, scatter, scheme drill-down |
| **Category Heatmap** | Flow heatmap (sub-category × month), cumulative flow by category |
| **Raw Data** | Full data table with CSV export, pipeline run log |

## Monthly Auto-Update

```bash
# Option 1: Run the scheduler as a background process
python scheduler.py
# Runs on 12th of every month at 09:30 IST (AMFI publishes data by ~10th)

# Option 2: Windows Task Scheduler / cron
# 0 9 12 * * /path/to/venv/bin/python /path/to/pipeline.py
```

## Key Configuration (pipeline.py)

| Constant | Value | Meaning |
|----------|-------|---------|
| `ICICI_MF_ID` | 17 | AMFI ID for ICICI Prudential AMC |
| `CATEGORY_EQUITY` | 1 | Investment type: Equity |
| `CATEGORY_HYBRID` | 3 | Investment type: Hybrid |
| `EQUITY_SUBCATEGORIES` | IDs 1-12 | Large Cap through Sectoral/Thematic |
| `HYBRID_SUBCATEGORIES` | IDs 30-35, 40 | Aggressive Hybrid through Balanced Hybrid |

## Known Limitations

- **Monthly cadence only** — AMFI publishes AUM monthly, not daily/weekly
- **NFO months** — newly launched schemes show their NFO collection as "inflow" (no prior AUM exists)
- **Mergers / reclassifications** — may cause anomalous flow spikes; check AMFI circulars
- **Currently single AMC** — only ICICI Prudential (mfid=17); see EXTENSION_PLAN.md for multi-AMC roadmap
