"""
Multi-AMC Flow Pipeline — System Level
========================================
Fetches data for ALL AMCs at once (mfid=0) from the AMFI-CRISIL API.
The API conveniently returns preMonthAUM and preNavRegular, so we can
compute flows from a single month's data without fetching the previous month.

    Expected_AUM(t) = preMonthAUM × [navRegular / preNavRegular]
    Net_Flow(t)     = dailyAUM − Expected_AUM(t)

Database: data/mf_flows_industry.db (separate from the ICICI-only DB)
"""

import os
import re
import sqlite3
import logging
import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "mf_flows_industry.db")

# ─────────────────────────────────────────────
# AMFI-CRISIL API CONFIGURATION
# ─────────────────────────────────────────────

API_BASE = "https://www.amfiindia.com/gateway/pollingsebi/api/amfi/"
API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.amfiindia.com",
    "Referer": "https://www.amfiindia.com/polling/amfi/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

# mfid=0 means ALL AMCs
SYSTEM_MFID = 0

MATURITY_TYPE_OPEN = 1

CATEGORIES = {
    1: "Equity",
    3: "Hybrid",
}

EQUITY_SUBCATEGORIES = {
    1: "Large Cap", 2: "Large & Mid Cap", 3: "Flexi Cap",
    4: "Multi Cap", 5: "Mid Cap", 6: "Small Cap",
    7: "Value", 8: "ELSS", 9: "Contra",
    10: "Dividend Yield", 11: "Focused", 12: "Sectoral / Thematic",
}

HYBRID_SUBCATEGORIES = {
    30: "Aggressive Hybrid", 31: "Conservative Hybrid",
    32: "Equity Savings", 33: "Arbitrage",
    34: "Multi Asset Allocation",
    35: "Dynamic Asset Allocation / Balanced Advantage",
    40: "Balanced Hybrid",
}

SUBCATEGORIES = {
    1: EQUITY_SUBCATEGORIES,
    3: HYBRID_SUBCATEGORIES,
}

MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Known AMC prefixes → short names (for extracting AMC from scheme name)
AMC_PREFIXES = [
    ("360 ONE ", "360 ONE"),
    ("Aditya Birla Sun Life ", "ABSL"),
    ("Angel One ", "Angel One"),
    ("Axis ", "Axis"),
    ("Bajaj Finserv ", "Bajaj Finserv"),
    ("Bandhan ", "Bandhan"),
    ("Bank of India ", "Bank of India"),
    ("Baroda BNP Paribas ", "Baroda BNP"),
    ("Canara Robeco ", "Canara Robeco"),
    ("Capitalmind ", "Capitalmind"),
    ("Choice ", "Choice"),
    ("DSP ", "DSP"),
    ("Edelweiss ", "Edelweiss"),
    ("Franklin India ", "Franklin"),
    ("Groww ", "Groww"),
    ("HDFC ", "HDFC"),
    ("HSBC ", "HSBC"),
    ("Helios ", "Helios"),
    ("ICICI Prudential ", "ICICI Pru"),
    ("ITI ", "ITI"),
    ("Invesco India ", "Invesco"),
    ("JM ", "JM Financial"),
    ("Jio BlackRock ", "Jio BlackRock"),
    ("Kotak ", "Kotak"),
    ("LIC MF ", "LIC"),
    ("Mahindra Manulife ", "Mahindra Manulife"),
    ("Mirae Asset ", "Mirae"),
    ("Motilal Oswal ", "Motilal Oswal"),
    ("NJ ", "NJ"),
    ("Navi ", "Navi"),
    ("Nippon India ", "Nippon"),
    ("Old Bridge ", "Old Bridge"),
    ("PGIM India ", "PGIM"),
    ("Parag Parikh ", "PPFAS"),
    ("Quant ", "Quant"),
    ("Quantum ", "Quantum"),
    ("SBI ", "SBI"),
    ("Samco ", "Samco"),
    ("Shriram ", "Shriram"),
    ("Sundaram ", "Sundaram"),
    ("Tata ", "Tata"),
    ("Taurus ", "Taurus"),
    ("Trust ", "Trust"),
    ("Union ", "Union"),
    ("UTI ", "UTI"),
    ("WhiteOak Capital ", "WhiteOak"),
    ("Zerodha ", "Zerodha"),
]


def extract_amc(scheme_name: str) -> str:
    """Extract AMC short name from scheme name."""
    for prefix, short in AMC_PREFIXES:
        if scheme_name.startswith(prefix):
            return short
    return "Other"


# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS industry_flows (
            amc             TEXT,
            scheme_name     TEXT,
            category        TEXT,
            sub_category    TEXT,
            month_end       TEXT,
            nav_cur         REAL,
            nav_prev        REAL,
            nav_return      REAL,
            aum_cur_cr      REAL,
            aum_prev_cr     REAL,
            expected_aum_cr REAL,
            net_flow_cr     REAL,
            flow_pct        REAL,
            PRIMARY KEY (amc, scheme_name, month_end)
        );

        CREATE TABLE IF NOT EXISTS pipeline_log (
            run_at          TEXT,
            month_processed TEXT,
            schemes_updated INTEGER,
            status          TEXT,
            message         TEXT
        );
    """)
    con.commit()
    con.close()
    log.info("Industry DB initialised at %s", DB_PATH)


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────

def _api_post(endpoint, payload):
    url = API_BASE + endpoint
    try:
        resp = requests.post(url, headers=API_HEADERS, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("validationMsg") != "SUCCESS":
            return {}
        return data
    except Exception as e:
        log.error("API call failed for %s: %s", endpoint, e)
        return {}


def get_last_business_day(year, month):
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    while last_day.weekday() >= 5:
        last_day -= timedelta(days=1)
    return last_day.strftime(f"%d-{MONTH_ABBR[last_day.month]}-%Y")


def fetch_all_system(report_date: str) -> pd.DataFrame:
    """
    Fetch ALL equity + hybrid schemes for ALL AMCs on a given date.
    Uses mfid=0 to get system-level data.
    Returns raw DataFrame with scheme_name, category, sub_category, navRegular, dailyAUM.
    """
    all_rows = []

    for cat_id, cat_name in CATEGORIES.items():
        subcats = SUBCATEGORIES[cat_id]
        for subcat_id, subcat_name in subcats.items():
            log.info("  Fetching %s > %s for %s ...", cat_name, subcat_name, report_date)
            payload = {
                "maturityType": MATURITY_TYPE_OPEN,
                "category": cat_id,
                "subCategory": subcat_id,
                "mfid": SYSTEM_MFID,
                "reportDate": report_date,
            }
            result = _api_post("fundperformance", payload)
            records = result.get("data", [])

            for rec in records:
                scheme = rec.get("schemeName", "")
                nav = rec.get("navRegular")
                aum = rec.get("dailyAUM")
                if not scheme or not nav or not aum:
                    continue

                all_rows.append({
                    "amc": extract_amc(scheme),
                    "scheme_name": scheme,
                    "category": cat_name,
                    "sub_category": subcat_name,
                    "nav_regular": nav,
                    "daily_aum_cr": aum,
                })

            time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    log.info("  Total schemes fetched for %s: %d", report_date, len(df))
    return df


def _try_fetch(year, month):
    """Try to fetch data for last business day of month, retrying earlier dates."""
    report_date = get_last_business_day(year, month)
    df = fetch_all_system(report_date)

    if df.empty:
        for offset in range(1, 5):
            dt = datetime.strptime(report_date, "%d-%b-%Y") - timedelta(days=offset)
            while dt.weekday() >= 5:
                dt -= timedelta(days=1)
            alt_date = dt.strftime(f"%d-{MONTH_ABBR[dt.month]}-%Y")
            log.info("  Retrying with %s ...", alt_date)
            df = fetch_all_system(alt_date)
            if not df.empty:
                report_date = alt_date
                break

    return df, report_date


# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────

def compute_flows_for_month(year: int, month: int):
    """
    Fetch current + previous month data for ALL AMCs, merge, compute flows.
    Same two-month approach as the ICICI pipeline but using mfid=0.
    """
    init_db()

    prev = date(year, month, 1) - relativedelta(months=1)

    log.info("=" * 60)
    log.info("Industry pipeline: %s-%d", MONTH_ABBR[month], year)
    log.info("=" * 60)

    # Fetch current month
    log.info("Fetching CURRENT month data...")
    df_cur, cur_date_str = _try_fetch(year, month)
    if df_cur.empty:
        msg = f"No data for current month ({year}-{month:02d})"
        log.error(msg)
        _log_run(f"{year}-{month:02d}", 0, "FAILED", msg)
        return

    # Fetch previous month
    log.info("Fetching PREVIOUS month data...")
    df_prev, prev_date_str = _try_fetch(prev.year, prev.month)
    if df_prev.empty:
        msg = f"No data for previous month ({prev.year}-{prev.month:02d})"
        log.error(msg)
        _log_run(f"{year}-{month:02d}", 0, "FAILED", msg)
        return

    cur_date_iso = datetime.strptime(cur_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    prev_date_iso = datetime.strptime(prev_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    # Merge on scheme_name
    merged = df_cur.merge(
        df_prev[["scheme_name", "nav_regular", "daily_aum_cr"]],
        on="scheme_name",
        how="inner",
        suffixes=("_cur", "_prev"),
    )

    if merged.empty:
        msg = "No matching schemes between months"
        log.error(msg)
        _log_run(cur_date_iso, 0, "FAILED", msg)
        return

    # Compute flows
    merged["nav_return"] = merged["nav_regular_cur"] / merged["nav_regular_prev"]
    merged["expected_aum_cr"] = merged["daily_aum_cr_prev"] * merged["nav_return"]
    merged["net_flow_cr"] = merged["daily_aum_cr_cur"] - merged["expected_aum_cr"]
    merged["flow_pct"] = (merged["net_flow_cr"] / merged["daily_aum_cr_prev"]) * 100

    # Build flow records
    flow_df = merged.rename(columns={
        "nav_regular_cur": "nav_cur",
        "nav_regular_prev": "nav_prev",
        "daily_aum_cr_cur": "aum_cur_cr",
        "daily_aum_cr_prev": "aum_prev_cr",
    })[["amc", "scheme_name", "category", "sub_category",
        "nav_cur", "nav_prev", "nav_return",
        "aum_cur_cr", "aum_prev_cr", "expected_aum_cr",
        "net_flow_cr", "flow_pct"]].copy()

    flow_df = flow_df.dropna(subset=["net_flow_cr"])
    flow_df = flow_df.drop_duplicates(subset=["amc", "scheme_name"], keep="first")
    flow_df["month_end"] = cur_date_iso

    # Store
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM industry_flows WHERE month_end = ?", (cur_date_iso,))
    con.commit()
    flow_df.to_sql("industry_flows", con, if_exists="append", index=False)
    con.commit()
    con.close()

    total_flow = flow_df["net_flow_cr"].sum()
    total_aum = flow_df["aum_cur_cr"].sum()
    n_amc = flow_df["amc"].nunique()
    log.info("=" * 60)
    log.info("Stored %d flow records for %s", len(flow_df), cur_date_iso)
    log.info("  AMCs:           %d", n_amc)
    log.info("  Total Net Flow: Rs %.0f Cr", total_flow)
    log.info("  Total AUM:      Rs %.0f Cr", total_aum)
    log.info("=" * 60)

    _log_run(cur_date_iso, len(flow_df), "SUCCESS", f"{n_amc} AMCs, {len(flow_df)} schemes")


def _log_run(month_end, count, status, message):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO pipeline_log (run_at, month_processed, schemes_updated, status, message) VALUES (?,?,?,?,?)",
        (datetime.utcnow().isoformat(), month_end, count, status, message),
    )
    con.commit()
    con.close()


# ─────────────────────────────────────────────
# QUERY HELPERS
# ─────────────────────────────────────────────

def load_flows(months=36):
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        f"SELECT * FROM industry_flows WHERE month_end >= date('now', '-{months} months') ORDER BY month_end DESC",
        con,
    )
    con.close()
    return df


def load_pipeline_log():
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM pipeline_log ORDER BY run_at DESC LIMIT 30", con)
    con.close()
    return df


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Industry-Level MF Flow Pipeline")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--backfill", type=int, default=0)
    args = parser.parse_args()

    if args.year and args.month:
        target_year, target_month = args.year, args.month
    else:
        today = date.today()
        target = date(today.year, today.month, 1) - relativedelta(months=1)
        target_year, target_month = target.year, target.month

    if args.backfill > 0:
        months_to_process = []
        for i in range(args.backfill, -1, -1):
            dt = date(target_year, target_month, 1) - relativedelta(months=i)
            months_to_process.append((dt.year, dt.month))

        log.info("Backfilling %d months", len(months_to_process))
        for yr, mn in months_to_process:
            compute_flows_for_month(yr, mn)
            time.sleep(1)
    else:
        compute_flows_for_month(target_year, target_month)
