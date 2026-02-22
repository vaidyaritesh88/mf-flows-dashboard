# Extension Plan: Multi-AMC & Sector-Level Flow Analysis

This document lays out a detailed plan to extend the current ICICI Prudential
single-AMC dashboard into a full **industry-level flow analysis platform** covering:

1. **Sector / Category-level flows** at the system level (all AMCs combined)
2. **Top 10 AMCs by AUM** — individual AMC flow dashboards
3. **Cross-AMC comparison** — who's gaining/losing market share

---

## Phase 1: Multi-AMC Data Pipeline

### 1.1 Discover all AMC IDs

The AMFI-CRISIL API has a `fundperformancefilters` endpoint that returns all AMCs:

```
POST https://www.amfiindia.com/gateway/pollingsebi/api/amfi/fundperformancefilters
Payload: {}
Response: { data: { mfList: [ { mfId: 17, mfName: "ICICI Prudential Mutual Fund" }, ... ] } }
```

**Action**: Call this endpoint once to get the full AMC list with IDs.

### 1.2 Identify Top 10 AMCs by AUM

The top 10 AMCs by equity + hybrid AUM (as of Jan 2026, approximate):

| Rank | AMC | Likely mfid | Approx Equity+Hybrid AUM |
|------|-----|-------------|--------------------------|
| 1 | SBI Mutual Fund | TBD | ~₹4.5L Cr |
| 2 | ICICI Prudential MF | 17 | ~₹3.2L Cr |
| 3 | HDFC Mutual Fund | TBD | ~₹3.0L Cr |
| 4 | Nippon India MF | TBD | ~₹2.0L Cr |
| 5 | Kotak Mahindra MF | TBD | ~₹1.8L Cr |
| 6 | Axis Mutual Fund | TBD | ~₹1.5L Cr |
| 7 | UTI Mutual Fund | TBD | ~₹1.2L Cr |
| 8 | DSP Mutual Fund | TBD | ~₹1.0L Cr |
| 9 | Mirae Asset MF | TBD | ~₹0.9L Cr |
| 10 | Tata Mutual Fund | TBD | ~₹0.8L Cr |

**Action**: Fetch `fundperformancefilters`, match AMC names, store the mfid mapping.

### 1.3 Parameterize the Pipeline

Currently `pipeline.py` hardcodes `ICICI_MF_ID = 17`. Changes needed:

```python
# NEW: pipeline_multi.py

# Configuration
TARGET_AMCS = {
    17: "ICICI Prudential",
    # ... other AMC IDs discovered in 1.2
}

# Or pass mfid=0 (or omit it) to get ALL AMCs combined for system-level view
SYSTEM_LEVEL_MFID = 0  # Need to test if API supports this

def fetch_schemes_for_date(report_date, category_id, subcategory_id,
                           subcategory_name, category_label, mfid):
    """Now accepts mfid as a parameter instead of hardcoding."""
    payload = {
        "maturityType": 1,
        "category": category_id,
        "subCategory": subcategory_id,
        "mfid": mfid,             # <-- parameterized
        "reportDate": report_date,
    }
    ...

def compute_flows_for_month(year, month, mfid, amc_name):
    """Compute flows for a specific AMC."""
    ...
```

### 1.4 Database Schema Changes

Add `amc_name` column to all tables:

```sql
CREATE TABLE monthly_snapshots_v2 (
    amc_name        TEXT,       -- NEW: e.g. 'ICICI Prudential', 'SBI', 'HDFC'
    scheme_name     TEXT,
    category        TEXT,
    sub_category    TEXT,
    month_end       TEXT,
    nav_regular     REAL,
    nav_direct      REAL,
    daily_aum_cr    REAL,
    PRIMARY KEY (amc_name, scheme_name, month_end)
);

CREATE TABLE monthly_flows_v2 (
    amc_name        TEXT,       -- NEW
    scheme_name     TEXT,
    category        TEXT,
    sub_category    TEXT,
    month_end       TEXT,
    prev_month_end  TEXT,
    nav_cur         REAL,
    nav_prev        REAL,
    nav_return      REAL,
    aum_cur_cr      REAL,
    aum_prev_cr     REAL,
    expected_aum_cr REAL,
    net_flow_cr     REAL,
    flow_pct        REAL,
    PRIMARY KEY (amc_name, scheme_name, month_end)
);
```

### 1.5 API Rate Limiting

Current pipeline makes ~19 API calls per AMC per month (12 equity + 7 hybrid subcategories).
For 10 AMCs = ~190 calls per month. Plus system-level = ~209 calls.

With 0.5s delay between calls = ~105 seconds per month of data.
Backfilling 13 months × 10 AMCs = ~23 minutes. Acceptable.

### 1.6 Testing the System-Level Query

**Key question**: Does the API support `mfid=0` or omitting `mfid` to get all AMCs at once?

- If **YES**: One call per subcategory returns all AMCs' schemes → much faster, ~19 calls total
- If **NO**: Must loop through all 40+ AMCs individually → slower but still feasible

**Action**: Test with `mfid=0`, `mfid=null`, and omitting `mfid` from the payload.

---

## Phase 2: Sector / Category-Level Analysis Dashboard

### 2.1 What Questions This Answers

- Which equity fund categories are seeing the highest inflows system-wide?
- Is money flowing into Large Cap or Small Cap this month?
- How are Sectoral/Thematic funds trending vs Flexi Cap?
- Which categories have sustained inflows vs one-off spikes?

### 2.2 Dashboard Views (new Streamlit app on port 8502)

**Page 1: System-Level Category Flows**

```
┌──────────────────────────────────────────────────────────────────┐
│ INDUSTRY-LEVEL MUTUAL FUND FLOW DASHBOARD                       │
│                                                                  │
│ KPIs: Total Industry AUM | Total Net Flow | Flow/AUM %          │
│                                                                  │
│ ┌─ Monthly Net Flow by Category (stacked bar) ─────────────────┐│
│ │  [Large Cap] [Mid Cap] [Small Cap] [Flexi Cap] [Thematic]... ││
│ │  Each bar = one month, stacked by category                   ││
│ └──────────────────────────────────────────────────────────────┘│
│                                                                  │
│ ┌─ Category Flow Ranking (horizontal bar) ─────────────────────┐│
│ │  Selected month: shows all categories ranked by net flow     ││
│ │  With absolute numbers + AUM for context                     ││
│ └──────────────────────────────────────────────────────────────┘│
│                                                                  │
│ ┌─ Category Trend Lines ──────────────────────────────────────┐ │
│ │  Multi-line chart: each line = one category's monthly flow  │ │
│ │  User can toggle categories on/off                          │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│ ┌─ Heatmap: Category × Month ─────────────────────────────────┐ │
│ │  Same as current Tab 3, but system-level (all AMCs combined)│ │
│ └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

**Page 2: Category Deep-Dive**

Select a category (e.g. "Sectoral / Thematic") and see:
- Total flow trend for that category
- Which AMCs are gaining/losing within this category
- Top 10 schemes by inflow within this category (across all AMCs)
- Top 10 schemes by AUM within this category
- Market share pie/bar: AMC-wise AUM split within this category

### 2.3 Key Charts

| Chart | X-axis | Y-axis | Purpose |
|-------|--------|--------|---------|
| Category flow time-series | Month | Net flow (₹ Cr) | Which categories getting money |
| Category flow ranking bar | Category | Net flow | Monthly winner/loser |
| Flow/AUM % by category | Category | % | Intensity of flows relative to size |
| Category heatmap | Month | Category | Quick visual scan |
| Category AUM trend | Month | AUM (₹ Cr) | Is the category growing? |
| AMC market share (within category) | AMC | AUM share % | Competitive landscape |

---

## Phase 3: AMC-Level Analysis

### 3.1 What Questions This Answers

- How is ICICI Pru doing vs SBI, HDFC, Nippon in equity flows?
- Which AMC gained the most market share this quarter?
- Are flows going to large AMCs or smaller ones?

### 3.2 Dashboard Views

**Page 3: AMC Comparison**

```
┌──────────────────────────────────────────────────────────────────┐
│ AMC COMPARISON                                                    │
│                                                                  │
│ ┌─ AMC Flow Ranking (selected month) ─────────────────────────┐ │
│ │  Horizontal bar: Top 10 AMCs by net flow                    │ │
│ │  Green = inflow, Red = outflow, with AUM context            │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│ ┌─ AMC Flow Trend (multi-line) ──────────────────────────────┐  │
│ │  Each line = one AMC's monthly net flow                    │  │
│ │  Toggle AMCs on/off                                        │  │
│ └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│ ┌─ Market Share Trend ──────────────────────────────────────┐   │
│ │  Stacked area: each AMC's AUM as % of total over time    │   │
│ └──────────────────────────────────────────────────────────┘   │
│                                                                  │
│ ┌─ AMC × Category Heatmap ─────────────────────────────────┐   │
│ │  Rows = AMCs, Columns = Categories                       │   │
│ │  Cell = net flow for that AMC in that category           │   │
│ └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

**Page 4: Single AMC Deep-Dive** (same as current app.py, but for any AMC)

- Select AMC from dropdown → see all charts for that AMC
- Same layout as current ICICI dashboard
- Comparison overlay: show selected AMC vs industry average

---

## Phase 4: Implementation Plan

### Step 1: API Discovery (Day 1)

- [ ] Call `fundperformancefilters` to get all AMC IDs
- [ ] Test `mfid=0` / omitting mfid for system-level data
- [ ] Map top 10 AMC names to their mfids
- [ ] Document any API quirks (rate limits, missing data, etc.)

### Step 2: Build pipeline_multi.py (Day 1-2)

- [ ] Copy pipeline.py → pipeline_multi.py
- [ ] Add `amc_name` parameter to all functions
- [ ] Add `AMC_MAP` dict: `{mfid: amc_name}`
- [ ] Create new DB tables with `amc_name` in PK
- [ ] Add CLI support: `--amc all` or `--amc "ICICI Prudential"`
- [ ] Add system-level aggregation logic
- [ ] Test with 2-3 AMCs for one month

### Step 3: Backfill Data (Day 2-3)

- [ ] Fetch 13 months of data for all 10 AMCs
- [ ] Verify data completeness (some AMCs may have fewer schemes)
- [ ] Store system-level aggregates
- [ ] Estimated time: ~25 minutes for full backfill

### Step 4: Build app_industry.py — Sector Dashboard (Day 3-4)

- [ ] New Streamlit app on **port 8502** (separate from current app on 8501)
- [ ] Page 1: System-level category flows
- [ ] Page 2: Category deep-dive
- [ ] Use `st.navigation` or `st.sidebar` for page switching
- [ ] Reuse chart patterns from current app.py

### Step 5: Build AMC Comparison Pages (Day 4-5)

- [ ] Page 3: AMC comparison (ranking, trend, market share)
- [ ] Page 4: Single AMC deep-dive (parameterized version of current app)
- [ ] AMC selector in sidebar

### Step 6: Integration & Polish (Day 5-6)

- [ ] Unified sidebar: AMC filter, Category filter, Date range
- [ ] CSV export for all views
- [ ] Performance optimization (cache heavy queries)
- [ ] Update scheduler.py to loop through all AMCs

---

## Phase 5: File Structure After Extension

```
Flows dashboard/
├── app.py                  # Current ICICI-only dashboard (port 8501) — UNCHANGED
├── app_industry.py         # NEW: Multi-AMC + sector dashboard (port 8502)
├── pipeline.py             # Current ICICI-only pipeline — UNCHANGED
├── pipeline_multi.py       # NEW: Multi-AMC pipeline
├── scheduler.py            # Updated: loops through all AMCs
├── amc_config.py           # NEW: AMC ID mapping + config
├── requirements.txt
├── README.md
├── ARCHITECTURE.md
├── EXTENSION_PLAN.md
└── data/
    ├── mf_flows.db         # Current ICICI-only DB — UNCHANGED
    └── mf_flows_industry.db # NEW: Multi-AMC database
```

### Running both dashboards simultaneously

```bash
# Terminal 1: Current ICICI dashboard (unchanged)
streamlit run app.py --server.port 8501

# Terminal 2: Industry dashboard (new)
streamlit run app_industry.py --server.port 8502
```

---

## Technical Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| API rate limiting | Slow/blocked fetches | 0.5s delay between calls; cache aggressively |
| API doesn't support mfid=0 | Can't get system-level in one call | Loop through all 40+ AMCs, aggregate in code |
| Missing data for some AMCs/months | Gaps in analysis | Log missing data; show "data unavailable" in UI |
| Large DB size (40+ AMCs × 40+ schemes × 13 months) | Slow queries | SQLite indexes; pre-aggregate category totals |
| Scheme name inconsistencies across months | Merge failures | Fuzzy matching on scheme name; use AMFI scheme code if available |

---

## Open Questions to Resolve

1. Does the API return an AMFI scheme code (unique ID) we can use instead of scheme_name?
   This would make cross-month merging more robust.

2. Can we get Debt fund categories too? (category IDs 2, 4, 5, etc.)
   This would give a complete industry picture.

3. Should we add Direct Plan flows separately?
   Currently we only track Regular Plan NAV for MTM benchmark.

4. Do we want to include ETFs and Index Funds?
   These are under different category IDs in the API.

---

## Estimated Effort

| Phase | Effort | Deliverable |
|-------|--------|-------------|
| Phase 1: Pipeline | 2 days | pipeline_multi.py + backfilled data |
| Phase 2: Sector dashboard | 2 days | Category-level analysis on port 8502 |
| Phase 3: AMC comparison | 2 days | AMC ranking + deep-dive pages |
| Phase 4: Polish | 1 day | Filters, export, performance |
| **Total** | **~7 days** | Full industry-level dashboard |
