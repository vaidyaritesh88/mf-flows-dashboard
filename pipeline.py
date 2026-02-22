"""
MF Flow Pipeline — ICICI Prudential Mutual Fund
=================================================
Fetches daily closing AUM and NAV data from the AMFI-CRISIL API
(https://www.amfiindia.com/gateway/pollingsebi/api/amfi/fundperformance)
and computes net monthly flows per scheme using:

    Expected_AUM(t) = AUM(t-1) × [NAV(t) / NAV(t-1)]
    Net_Flow(t)     = AUM(t) − Expected_AUM(t)

where NAV refers to the Regular Plan Growth NAV from the same API.
"""

import os
import sqlite3
import logging
import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "mf_flows.db")

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
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# ICICI Prudential Mutual Fund ID in the AMFI system
ICICI_MF_ID = 17

# Open Ended = 1
MATURITY_TYPE_OPEN = 1

# Investment type IDs: Equity=1, Hybrid=3
CATEGORY_EQUITY = 1
CATEGORY_HYBRID = 3

# Equity sub-categories
EQUITY_SUBCATEGORIES = {
    1: "Large Cap",
    2: "Large & Mid Cap",
    3: "Flexi Cap",
    4: "Multi Cap",
    5: "Mid Cap",
    6: "Small Cap",
    7: "Value",
    8: "ELSS",
    9: "Contra",
    10: "Dividend Yield",
    11: "Focused",
    12: "Sectoral / Thematic",
}

# Hybrid sub-categories
HYBRID_SUBCATEGORIES = {
    30: "Aggressive Hybrid",
    31: "Conservative Hybrid",
    32: "Equity Savings",
    33: "Arbitrage",
    34: "Multi Asset Allocation",
    35: "Dynamic Asset Allocation / Balanced Advantage",
    40: "Balanced Hybrid",
}

MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS monthly_snapshots (
            scheme_name     TEXT,
            category        TEXT,       -- 'Equity' or 'Hybrid'
            sub_category    TEXT,       -- e.g. 'Large Cap', 'Flexi Cap'
            month_end       TEXT,       -- YYYY-MM-DD (actual trading day)
            nav_regular     REAL,
            nav_direct      REAL,
            daily_aum_cr    REAL,       -- closing AUM on that day (₹ Cr)
            PRIMARY KEY (scheme_name, month_end)
        );

        CREATE TABLE IF NOT EXISTS monthly_flows (
            scheme_name     TEXT,
            category        TEXT,
            sub_category    TEXT,
            month_end       TEXT,       -- current month end date
            prev_month_end  TEXT,       -- previous month end date
            nav_cur         REAL,       -- Regular NAV current month end
            nav_prev        REAL,       -- Regular NAV previous month end
            nav_return      REAL,       -- nav_cur / nav_prev
            aum_cur_cr      REAL,       -- AUM current month
            aum_prev_cr     REAL,       -- AUM previous month
            expected_aum_cr REAL,       -- aum_prev * nav_return
            net_flow_cr     REAL,       -- aum_cur - expected_aum
            flow_pct        REAL,       -- net_flow / aum_prev * 100
            PRIMARY KEY (scheme_name, month_end)
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
    log.info("Database initialised at %s", DB_PATH)


# ─────────────────────────────────────────────
# API HELPERS
# ─────────────────────────────────────────────

def _api_post(endpoint: str, payload: dict) -> dict:
    """POST to the AMFI-CRISIL API and return parsed JSON."""
    url = API_BASE + endpoint
    try:
        resp = requests.post(url, headers=API_HEADERS, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if data.get("validationMsg") != "SUCCESS":
            log.warning("API returned non-success: %s", data.get("errorMsgs"))
            return {}
        return data
    except Exception as e:
        log.error("API call failed for %s: %s", endpoint, e)
        return {}


def get_last_business_day(year: int, month: int) -> str:
    """
    Returns the last business day of the given month as 'dd-MMM-yyyy'.
    Walks backward from the last calendar day, skipping Sat/Sun.
    """
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)

    # Walk back to a weekday
    while last_day.weekday() >= 5:  # 5=Sat, 6=Sun
        last_day -= timedelta(days=1)

    return last_day.strftime(f"%d-{MONTH_ABBR[last_day.month]}-%Y")


def fetch_schemes_for_date(report_date: str, category_id: int,
                           subcategory_id: int, subcategory_name: str,
                           category_label: str) -> list:
    """
    Fetches all ICICI Pru schemes for a given sub-category on a specific date.
    Returns list of dicts with scheme-level data.
    """
    payload = {
        "maturityType": MATURITY_TYPE_OPEN,
        "category": category_id,
        "subCategory": subcategory_id,
        "mfid": ICICI_MF_ID,
        "reportDate": report_date,
    }
    result = _api_post("fundperformance", payload)
    records = result.get("data", [])

    if not records:
        return []

    rows = []
    for rec in records:
        rows.append({
            "scheme_name": rec.get("schemeName"),
            "category": category_label,
            "sub_category": subcategory_name,
            "nav_date": rec.get("navDate"),
            "nav_regular": rec.get("navRegular"),
            "nav_direct": rec.get("navDirect"),
            "daily_aum_cr": rec.get("dailyAUM"),
        })
    return rows


def fetch_all_schemes_for_date(report_date: str) -> pd.DataFrame:
    """
    Fetches all ICICI Pru equity + hybrid schemes for a given date.
    Iterates over all sub-categories.
    """
    all_rows = []

    # Equity sub-categories
    for subcat_id, subcat_name in EQUITY_SUBCATEGORIES.items():
        log.info("  Fetching Equity > %s for %s ...", subcat_name, report_date)
        rows = fetch_schemes_for_date(report_date, CATEGORY_EQUITY,
                                      subcat_id, subcat_name, "Equity")
        all_rows.extend(rows)
        time.sleep(0.3)  # Be polite to the API

    # Hybrid sub-categories
    for subcat_id, subcat_name in HYBRID_SUBCATEGORIES.items():
        log.info("  Fetching Hybrid > %s for %s ...", subcat_name, report_date)
        rows = fetch_schemes_for_date(report_date, CATEGORY_HYBRID,
                                      subcat_id, subcat_name, "Hybrid")
        all_rows.extend(rows)
        time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        log.info("  Total schemes fetched for %s: %d", report_date, len(df))
    else:
        log.warning("  No data returned for %s", report_date)
    return df


# ─────────────────────────────────────────────
# SNAPSHOT STORAGE
# ─────────────────────────────────────────────

def store_snapshot(df: pd.DataFrame, month_end_iso: str):
    """Store month-end snapshot into monthly_snapshots table."""
    if df.empty:
        return

    con = sqlite3.connect(DB_PATH)

    # Delete existing records for this month_end first to avoid PK conflicts
    con.execute("DELETE FROM monthly_snapshots WHERE month_end = ?", (month_end_iso,))

    df_store = df[["scheme_name", "category", "sub_category",
                    "nav_regular", "nav_direct", "daily_aum_cr"]].copy()
    df_store["month_end"] = month_end_iso

    df_store.to_sql("monthly_snapshots", con, if_exists="append", index=False)
    con.commit()
    con.close()
    log.info("Stored %d snapshot records for %s", len(df_store), month_end_iso)


# ─────────────────────────────────────────────
# FLOW COMPUTATION
# ─────────────────────────────────────────────

def compute_flows_for_month(year: int, month: int):
    """
    Main entry point: fetches month-end data for the given month and the
    previous month, computes net flows, stores everything.
    """
    init_db()

    # Current month end
    cur_date_str = get_last_business_day(year, month)
    prev = date(year, month, 1) - relativedelta(months=1)
    prev_date_str = get_last_business_day(prev.year, prev.month)

    log.info("═" * 60)
    log.info("Computing flows for %s-%d", MONTH_ABBR[month], year)
    log.info("  Current month-end: %s", cur_date_str)
    log.info("  Previous month-end: %s", prev_date_str)
    log.info("═" * 60)

    # Fetch current month data
    log.info("Fetching current month data (%s) ...", cur_date_str)
    df_cur = fetch_all_schemes_for_date(cur_date_str)

    if df_cur.empty:
        # Try walking back a couple of days (holidays)
        for offset in range(1, 5):
            dt = datetime.strptime(cur_date_str, "%d-%b-%Y") - timedelta(days=offset)
            while dt.weekday() >= 5:
                dt -= timedelta(days=1)
            alt_date = dt.strftime(f"%d-{MONTH_ABBR[dt.month]}-%Y")
            log.info("  Retrying with %s ...", alt_date)
            df_cur = fetch_all_schemes_for_date(alt_date)
            if not df_cur.empty:
                cur_date_str = alt_date
                break

    if df_cur.empty:
        msg = f"No data available for current month ({cur_date_str})"
        log.error(msg)
        _log_run(cur_date_str, 0, "FAILED", msg)
        return

    # Fetch previous month data
    log.info("Fetching previous month data (%s) ...", prev_date_str)
    df_prev = fetch_all_schemes_for_date(prev_date_str)

    if df_prev.empty:
        for offset in range(1, 5):
            dt = datetime.strptime(prev_date_str, "%d-%b-%Y") - timedelta(days=offset)
            while dt.weekday() >= 5:
                dt -= timedelta(days=1)
            alt_date = dt.strftime(f"%d-{MONTH_ABBR[dt.month]}-%Y")
            log.info("  Retrying with %s ...", alt_date)
            df_prev = fetch_all_schemes_for_date(alt_date)
            if not df_prev.empty:
                prev_date_str = alt_date
                break

    if df_prev.empty:
        msg = f"No data available for previous month ({prev_date_str})"
        log.error(msg)
        _log_run(cur_date_str, 0, "FAILED", msg)
        return

    # Parse actual date used
    cur_date_iso = datetime.strptime(cur_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    prev_date_iso = datetime.strptime(prev_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    # Store snapshots
    store_snapshot(df_cur, cur_date_iso)
    store_snapshot(df_prev, prev_date_iso)

    # Merge current and previous month
    merged = df_cur.merge(
        df_prev[["scheme_name", "nav_regular", "daily_aum_cr"]],
        on="scheme_name",
        how="inner",
        suffixes=("_cur", "_prev"),
    )

    if merged.empty:
        msg = "No matching schemes between current and previous month"
        log.error(msg)
        _log_run(cur_date_iso, 0, "FAILED", msg)
        return

    # Rename for clarity
    merged = merged.rename(columns={
        "nav_regular_cur": "nav_cur",
        "nav_regular_prev": "nav_prev",
        "daily_aum_cr_cur": "aum_cur_cr",
        "daily_aum_cr_prev": "aum_prev_cr",
    })

    # Compute flows
    merged["nav_return"] = merged["nav_cur"] / merged["nav_prev"]
    merged["expected_aum_cr"] = merged["aum_prev_cr"] * merged["nav_return"]
    merged["net_flow_cr"] = merged["aum_cur_cr"] - merged["expected_aum_cr"]
    merged["flow_pct"] = (merged["net_flow_cr"] / merged["aum_prev_cr"]) * 100

    # Store flows
    con = sqlite3.connect(DB_PATH)
    flow_records = []
    for _, row in merged.iterrows():
        if pd.isna(row.get("net_flow_cr")):
            continue
        flow_records.append({
            "scheme_name": row["scheme_name"],
            "category": row["category"],
            "sub_category": row["sub_category"],
            "month_end": cur_date_iso,
            "prev_month_end": prev_date_iso,
            "nav_cur": row["nav_cur"],
            "nav_prev": row["nav_prev"],
            "nav_return": row["nav_return"],
            "aum_cur_cr": row["aum_cur_cr"],
            "aum_prev_cr": row["aum_prev_cr"],
            "expected_aum_cr": row["expected_aum_cr"],
            "net_flow_cr": row["net_flow_cr"],
            "flow_pct": row["flow_pct"],
        })

    if flow_records:
        flow_df = pd.DataFrame(flow_records)

        # Delete existing records for this month to avoid PK conflicts on re-run
        con.execute("DELETE FROM monthly_flows WHERE month_end = ?", (cur_date_iso,))
        con.commit()

        flow_df.to_sql("monthly_flows", con, if_exists="append", index=False)
        con.commit()

    con.close()

    log.info("═" * 60)
    log.info("Stored %d flow records for %s", len(flow_records), cur_date_iso)
    total_flow = sum(r["net_flow_cr"] for r in flow_records)
    total_aum = sum(r["aum_cur_cr"] for r in flow_records)
    log.info("  Total Net Flow: ₹%.0f Cr", total_flow)
    log.info("  Total AUM:      ₹%.0f Cr", total_aum)
    log.info("═" * 60)

    _log_run(cur_date_iso, len(flow_records), "SUCCESS", "")


def _log_run(month_end: str, count: int, status: str, message: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO pipeline_log (run_at, month_processed, schemes_updated, status, message)
        VALUES (?, ?, ?, ?, ?)
    """, (datetime.utcnow().isoformat(), month_end, count, status, message))
    con.commit()
    con.close()


# ─────────────────────────────────────────────
# QUERY HELPERS (used by dashboard)
# ─────────────────────────────────────────────

def get_con():
    return sqlite3.connect(DB_PATH)


def load_flows(months: int = 36) -> pd.DataFrame:
    con = get_con()
    q = f"""
        SELECT *
        FROM monthly_flows
        WHERE month_end >= date('now', '-{months} months')
        ORDER BY month_end DESC
    """
    df = pd.read_sql(q, con)
    con.close()
    return df


def load_all_months() -> list:
    con = get_con()
    rows = con.execute(
        "SELECT DISTINCT month_end FROM monthly_flows ORDER BY month_end DESC"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def load_pipeline_log() -> pd.DataFrame:
    con = get_con()
    df = pd.read_sql("SELECT * FROM pipeline_log ORDER BY run_at DESC LIMIT 20", con)
    con.close()
    return df


def load_snapshots() -> pd.DataFrame:
    con = get_con()
    df = pd.read_sql("SELECT * FROM monthly_snapshots ORDER BY month_end DESC", con)
    con.close()
    return df


# ─────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="MF Flow Pipeline")
    parser.add_argument("--year", type=int, help="Year (e.g. 2025)")
    parser.add_argument("--month", type=int, help="Month (1-12)")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Number of months to backfill from the target month")
    args = parser.parse_args()

    if args.year and args.month:
        target_year, target_month = args.year, args.month
    else:
        # Default: previous month
        today = date.today()
        target = date(today.year, today.month, 1) - relativedelta(months=1)
        target_year, target_month = target.year, target.month

    if args.backfill > 0:
        # Backfill: process multiple months from oldest to newest
        months_to_process = []
        for i in range(args.backfill, -1, -1):
            dt = date(target_year, target_month, 1) - relativedelta(months=i)
            months_to_process.append((dt.year, dt.month))

        log.info("Backfilling %d months: %s to %s",
                 len(months_to_process),
                 f"{months_to_process[0][1]}/{months_to_process[0][0]}",
                 f"{months_to_process[-1][1]}/{months_to_process[-1][0]}")

        for yr, mn in months_to_process:
            compute_flows_for_month(yr, mn)
            time.sleep(1)  # Rate limiting between months
    else:
        log.info("Running pipeline for %s/%s", target_month, target_year)
        compute_flows_for_month(target_year, target_month)
