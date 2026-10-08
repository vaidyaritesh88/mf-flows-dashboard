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

## Static HTML dashboard (no server needed)

`build_dashboard.py` reads `data/mf_flows_industry.db` and writes **`docs/index.html`** —
a single self-contained file with every month embedded (≈1.6 MB). Open it directly in a
browser, share it, or host it on GitHub Pages (Settings → Pages → Deploy from branch
`main`, folder `/docs`). Light theme by default; the header button switches to dark.

```bash
python pipeline_multi.py --year 2026 --month 8   # pull a month (or omit args = previous month)
python fetch_index_levels.py                     # refresh Nifty month-end closes for the AUM-bridge tab
python fetch_amc_prices.py                       # refresh listed-AMC share prices for the Valuation tab
python build_dashboard.py                        # regenerate docs/index.html
git add data docs && git commit -m "Data through Aug 2026" && git push
```

The **AUM bridge** tab splits the AUM change between any two month-ends into net inflows, NFOs /
scheme changes and mark-to-market for any set of AMCs (Industry by default), with the chained
AUM-weighted NAV return against Nifty 50 / 500 / Midcap 150 / Smallcap 250 price returns
(`data/index_levels.json`, from Yahoo Finance). `pipeline_multi.py` guards against NAV-series
breaks in the AMFI feed (`NAV_BREAK_TOL`): a scheme-month whose NAV return sits more than 25 points
from its sub-category median is replaced by that median.

`.github/workflows/refresh-data.yml` runs those steps automatically on the 12th of
every month (and on demand from the Actions tab), committing the refreshed DB and HTML.

The Streamlit apps (`app.py`, `app_industry.py`) still work and read the same DB.

## Quarterly AUM tab (daily-average basis)

`pipeline_daily.py` samples scheme-level daily AUM from the AMFI feed for every trading day
(table `daily_aum`, resumable, ~20 s per day). The **Quarterly AUM** tab averages those daily
values over each financial quarter — the same basis as the quarterly average AUM the AMCs report —
and shows QoQ and YoY for the selected quarter, plus a daily AUM chart with the quarter averages.

```bash
python pipeline_daily.py --from 2025-07-01 --to 2026-10-08   # one-off backfill (quarters with < 40 days are flagged partial)
python pipeline_daily.py                                      # top-up: last two weeks (run with the monthly refresh)
```

## Performance tab (scheme quartiles vs AMFI peers and benchmarks)

`pipeline_multi.py` also stores each scheme's trailing 1/3/5-year returns (regular and direct plan)
and its benchmark's return, as published by the AMFI-CRISIL feed, in the `scheme_returns` table.
The monthly run does this automatically; to (re)build history run once:

```bash
python pipeline_multi.py --returns-backfill   # ~1 hour for 90 months; resumable, skips months already stored
```

The **Performance** tab ranks every scheme within its AMFI sub-category each month, buckets it into
quartiles (categories with fewer than 8 schemes are ranked but not bucketed), and shows the share of
each AMC's AUM by quartile over time, the share beating its own benchmark, AUM-weighted alpha, an AMC
scorecard, a top-half heatmap and a scheme-level table.

## Valuation tab

`fetch_amc_prices.py` pulls split-adjusted prices for ICICIAMC, HDFCAMC, NAM-INDIA and SBIFUNDS (Yahoo
Finance) into `data/amc_prices.json`. The tab shows trailing-12-month P/E history (market cap on the
latest diluted share count ÷ trailing four quarters of PAT, reported or excluding other income), and a
snapshot with FY26 and FY27E P/E where FY27E PAT = FY26 × (1 + an editable per-company growth %, default 14%) for HDFC, Nippon and SBI, and the Janchor model's FY27E PAT for ICICI Pru.

The **Data** tab also carries a *Period time series* table (industry or one AMC, at the selected
frequency: monthly / quarterly / financial year) with net inflow, opening and closing AUM,
mark-to-market, the NFO residual, and net inflow by sub-category, plus a CSV download for modelling.

## Listed AMCs tab (company financials)

`build_amc_data.py` compiles a standardised P&L + AAUM dataset for ICICI Pru AMC, HDFC AMC,
Nippon Life India AMC, SBI Funds Management and Canara Robeco AMC from the latest sell-side /
Janchor models saved under `Janchor\BFSI\Financial Services\AMCs`, into `data/amc_financials.json`.
The dashboard's **Listed AMCs** tab reads that file: quarterly or annual, ₹ mn or bps of AAUM,
a three-way AAUM base (total / ex EPFO-type / MF only), and an off-by-default toggle for the ICICI Pru forecast years (FY27E–FY31E from the Janchor model).

Quarterly refresh (after results, ~mid Jul / Oct / Jan / Apr):
1. Save the updated models and point the `MODEL` paths at the top of `build_amc_data.py` to them
   (row/column positions are documented inline; check them if a broker changes the layout).
2. `python build_amc_data.py` — prints the period coverage per company; eyeball the PBT check.
3. `python build_dashboard.py` and commit `data/` + `docs/`.

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
