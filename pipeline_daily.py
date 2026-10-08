"""
Daily AUM sampler (for true quarterly-average AUM).
====================================================
Fetches scheme-level daily AUM from the AMFI-CRISIL feed for every weekday in a date range and
stores it in table daily_aum (date, amc, scheme_name, category, sub_category, aum_cr).
Holidays return no data and are skipped. Resumable: dates already stored are not re-fetched.

Usage:
    python pipeline_daily.py --from 2025-07-01 --to 2026-10-08      # explicit range
    python pipeline_daily.py                                          # last 10 weekdays (keeps the current quarter topped up)

Each day costs 19 API calls (~20-25 s); a quarter is ~62 days.
"""
import argparse, sqlite3, time, logging
from datetime import date, datetime, timedelta
import pipeline_multi as pl

log = logging.getLogger("daily")


def init():
    con = sqlite3.connect(pl.DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS daily_aum (
            date         TEXT,
            amc          TEXT,
            scheme_name  TEXT,
            category     TEXT,
            sub_category TEXT,
            aum_cr       REAL,
            PRIMARY KEY (date, amc, scheme_name)
        );
        CREATE TABLE IF NOT EXISTS daily_aum_log (date TEXT PRIMARY KEY, schemes INTEGER, fetched_at TEXT);
    """)
    con.commit()
    con.close()


def weekdays(d0, d1):
    d = d0
    while d <= d1:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def run(d0, d1):
    init()
    con = sqlite3.connect(pl.DB_PATH)
    done = {r[0] for r in con.execute("SELECT date FROM daily_aum_log")}
    todo = [d for d in weekdays(d0, d1) if d.isoformat() not in done]
    log.info("Daily AUM: %d weekdays to fetch between %s and %s (%d already stored)", len(todo), d0, d1, len(done))
    for d in todo:
        rd = d.strftime(f"%d-{pl.MONTH_ABBR[d.month]}-%Y")
        df = pl.fetch_all_system(rd)
        n = 0
        if not df.empty:
            out = df[["amc", "scheme_name", "category", "sub_category", "daily_aum_cr"]].rename(columns={"daily_aum_cr": "aum_cr"})
            out = out.drop_duplicates(subset=["amc", "scheme_name"], keep="first").copy()
            out.insert(0, "date", d.isoformat())
            con.execute("DELETE FROM daily_aum WHERE date=?", (d.isoformat(),))
            out.to_sql("daily_aum", con, if_exists="append", index=False)
            n = len(out)
        con.execute("INSERT OR REPLACE INTO daily_aum_log VALUES (?,?,?)", (d.isoformat(), n, datetime.now().isoformat(timespec="seconds")))
        con.commit()
        log.info("  %s: %d schemes%s", d.isoformat(), n, "" if n else " (holiday / no data)")
        time.sleep(0.5)
    con.close()


def clean():
    """Repair unit errors in the daily feed.
    (1) A scheme whose daily level is a power of ten away from its month-end AUM (industry_flows) is rescaled.
    (2) A scheme-day more than 4x above or 4x below the scheme's median daily AUM is dropped as a spike."""
    con = sqlite3.connect(pl.DB_PATH)
    fixed_scale, dropped = 0, 0
    # (1) scale check against month-end AUM for the same scheme
    rows = con.execute("""
        with d as (select amc, scheme_name, avg(aum_cr) dm from daily_aum group by 1,2),
             m as (select amc, scheme_name, avg(aum_cur_cr) mm from industry_flows where month_end >= (select min(date) from daily_aum) group by 1,2)
        select d.amc, d.scheme_name, d.dm, m.mm from d join m using (amc, scheme_name) where m.mm > 0 and d.dm > 0""").fetchall()
    import math
    for amc, name, dm, mm in rows:
        k = round(math.log10(dm / mm))
        if abs(k) >= 2:                                   # off by 100x or more: a unit error, not a real move
            con.execute("UPDATE daily_aum SET aum_cr = aum_cr / ? WHERE amc=? AND scheme_name=?", (10 ** k, amc, name))
            fixed_scale += 1
            log.info("  rescaled %s / %s by 10^%d", amc, name, -k)
    con.commit()
    # (2) one-day spikes: a scheme-day more than 3x away from the median of its neighbouring +/-5 trading
    #     days (a LOCAL median, so genuine growth paths of young schemes are left alone)
    import statistics
    by = {}
    for amc, name, d, v in con.execute("SELECT amc, scheme_name, date, aum_cr FROM daily_aum ORDER BY date"):
        by.setdefault((amc, name), []).append((d, v))
    for (amc, name), vals in by.items():
        if len(vals) < 7:
            continue
        bad = []
        for i, (d, v) in enumerate(vals):
            nb = [x for j, (_, x) in enumerate(vals) if j != i and abs(j - i) <= 5]
            med = statistics.median(nb)
            if med > 0 and (v > 3 * med or v < med / 3):
                bad.append(d)
        for d in bad:
            con.execute("DELETE FROM daily_aum WHERE amc=? AND scheme_name=? AND date=?", (amc, name, d))
        dropped += len(bad)
    con.commit()
    con.close()
    log.info("clean: %d schemes rescaled, %d scheme-days dropped as spikes", fixed_scale, dropped)
    return fixed_scale, dropped


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="d0")
    ap.add_argument("--to", dest="d1")
    a = ap.parse_args()
    d1 = date.fromisoformat(a.d1) if a.d1 else date.today()
    d0 = date.fromisoformat(a.d0) if a.d0 else d1 - timedelta(days=14)
    run(d0, d1)
    clean()
