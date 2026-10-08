"""
Build the static MF Industry Flows dashboard.
==============================================
Reads data/mf_flows_industry.db, embeds all months as compact JSON into a
single self-contained HTML file (docs/index.html) that needs no server.

Usage:
    python build_dashboard.py                 # writes docs/index.html
    python build_dashboard.py --out path.html # custom output path

Refresh cycle (monthly, after AMFI publishes ~10th):
    python pipeline_multi.py            # pulls previous month into the DB
    python build_dashboard.py           # regenerates docs/index.html
    git add data docs && git commit -m "Data through <Mon YYYY>" && git push
"""
import argparse, json, os, sqlite3
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "data", "mf_flows_industry.db")
TEMPLATE = os.path.join(HERE, "dashboard_template.html")
DEFAULT_OUT = os.path.join(HERE, "docs", "index.html")

EQUITY = {"Large Cap", "Large & Mid Cap", "Flexi Cap", "Multi Cap", "Mid Cap", "Small Cap",
          "Value", "ELSS", "Contra", "Dividend Yield", "Focused", "Sectoral / Thematic"}


def load_payload():
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT amc, scheme_name, category, sub_category, month_end, "
        "net_flow_cr, aum_cur_cr, aum_prev_cr, nav_return FROM industry_flows"
    ).fetchall()
    con.close()

    months = sorted({r[4] for r in rows})
    m_idx = {m: i for i, m in enumerate(months)}

    # AMC index ordered by latest-month AUM (largest first) so charts default sensibly
    last = months[-1]
    amc_aum = {}
    for r in rows:
        if r[4] == last:
            amc_aum[r[0]] = amc_aum.get(r[0], 0) + (r[6] or 0)
    amcs = sorted({r[0] for r in rows}, key=lambda a: -amc_aum.get(a, 0))
    a_idx = {a: i for i, a in enumerate(amcs)}

    cats = sorted({(r[3], r[2]) for r in rows}, key=lambda c: (c[1], c[0]))
    c_idx = {c[0]: i for i, c in enumerate(cats)}

    schemes, s_idx = [], {}
    data = []
    for amc, name, cat, sub, me, flow, aum, aum_prev, navret in rows:
        key = (amc, name)
        if key not in s_idx:
            s_idx[key] = len(schemes)
            schemes.append([name, a_idx[amc], c_idx[sub]])
        data.append([s_idx[key], m_idx[me], round(flow or 0, 1), round(aum or 0, 1),
                     round(aum_prev or 0, 1), round(navret or 0, 5)])
    data.sort(key=lambda d: (d[1], d[0]))

    # ── Scheme renames: a scheme that leaves under one name and re-enters next month under another
    # (same AMC, same sub-category, opening AUM equal to the old name's closing AUM) is the same scheme.
    # Map the old index onto the new one so scheme-level history is continuous and renames stop
    # showing up as NFO entries / exits.
    by_scheme = {}
    for d in data:
        by_scheme.setdefault(d[0], {})[d[1]] = d
    first = {si: min(ms) for si, ms in by_scheme.items()}
    last = {si: max(ms) for si, ms in by_scheme.items()}
    alias, renames = {}, []
    for mi in range(1, len(months)):
        entries = [si for si in by_scheme if first[si] == mi and by_scheme[si][mi][4] > 0]
        exits = [si for si in by_scheme if last[si] == mi - 1]
        used = set()
        for e in entries:
            best, best_err = None, 0.03            # AMFI restates prior-month AUM by up to ~3% at renames
            for x in exits:
                if x in used or schemes[x][1] != schemes[e][1] or schemes[x][2] != schemes[e][2]:
                    continue
                old_close = by_scheme[x][mi - 1][3]
                if not old_close:
                    continue
                err = abs(by_scheme[e][mi][4] - old_close) / old_close
                if err < best_err:
                    best, best_err = x, err
            if best is not None:
                used.add(best)
                alias[best] = e
                renames.append((schemes[best][0], schemes[e][0], months[mi]))
    if alias:
        def canon(si):
            while si in alias:
                si = alias[si]
            return si
        for d in data:
            d[0] = canon(d[0])
        data.sort(key=lambda d: (d[1], d[0]))
        for old, new in alias.items():
            s_idx[(schemes[old][1], schemes[old][0])] = canon(old)     # returns rows keyed by old name map to the new scheme
    print(f"scheme renames merged: {len(renames)}")
    for a, b, m in renames[:8]:
        print(f"   {m}: {a}  ->  {b}")

    # trailing returns (scheme_returns), if the backfill has run: [schemeIdx, monthIdx, r1y, r3y, r5y, b1y, b3y, b5y, aum]
    perf, bench = [], {}
    con = sqlite3.connect(DB_PATH)
    has = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scheme_returns'").fetchone()
    if has:
        rr = con.execute("SELECT amc, scheme_name, category, sub_category, month_end, aum_cr, benchmark, "
                         "r1y, r3y, r5y, b1y, b3y, b5y FROM scheme_returns ORDER BY month_end").fetchall()
        for amc, name, cat, sub, me, aum, bm, r1, r3, r5, b1, b3, b5 in rr:
            if me not in m_idx or sub not in c_idx:
                continue
            key = (amc, name)
            if key not in s_idx:
                if amc not in a_idx:
                    a_idx[amc] = len(amcs); amcs.append(amc)
                s_idx[key] = len(schemes)
                schemes.append([name, a_idx[amc], c_idx[sub]])
            si = s_idx[key]
            if bm:
                bench[si] = bm                       # latest benchmark name wins (rows ordered by month)
            rd = lambda v: None if v is None else round(v, 2)
            perf.append([si, m_idx[me], rd(r1), rd(r3), rd(r5), rd(b1), rd(b3), rd(b5), round(aum or 0, 1)])
    con.close()
    for si, bm in bench.items():
        if len(schemes[si]) == 3:
            schemes[si].append(bm)

    # is the latest month a month-to-date snapshot? (pipeline_log records the AMFI date actually used)
    last_data_date = None
    con = sqlite3.connect(DB_PATH)
    lg = con.execute("SELECT message FROM pipeline_log WHERE month_processed=? AND status='SUCCESS' ORDER BY run_at DESC LIMIT 1",
                     (months[-1],)).fetchone()
    con.close()
    if lg and "data from " in lg[0]:
        last_data_date = lg[0].split("data from ")[1].rstrip(")").strip()
    provisional = bool(last_data_date) and last_data_date < months[-1]

    # daily AUM (pipeline_daily.py) aggregated to AMC x day x {equity ex-arb, hybrid ex-arb, arbitrage}
    daily = None
    con = sqlite3.connect(DB_PATH)
    if con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_aum'").fetchone():
        # scheme-level, then forward-fill a scheme's AUM over days it is missing from the feed (feed gaps
        # and dropped spikes) between its first and last appearance, so AMC totals are not biased down
        dates = [r[0] for r in con.execute("SELECT date FROM daily_aum_log WHERE schemes > 0 ORDER BY 1")]
        d_idx = {d: i for i, d in enumerate(dates)}
        q = con.execute("SELECT amc, scheme_name, CASE WHEN sub_category='Arbitrage' THEN 2 WHEN category='Equity' THEN 0 ELSE 1 END g, date, aum_cr "
                        "FROM daily_aum ORDER BY amc, scheme_name, date").fetchall()
        if q and dates:
            ser, cur_key, series = {}, None, None
            def flush(key, series):
                if not key or key[0] not in a_idx:
                    return
                arr = ser.setdefault(a_idx[key[0]], [[0, 0, 0] for _ in dates])
                idxs = sorted(series)
                lo, hi, last = idxs[0], idxs[-1], None
                for i in range(lo, hi + 1):
                    if i in series:
                        last = series[i]
                    arr[i][key[2]] += last
            for amc, name, g, d, v in q:
                key = (amc, name, g)
                if key != cur_key:
                    flush(cur_key, series or {})
                    cur_key, series = key, {}
                if d in d_idx:
                    series[d_idx[d]] = v
            flush(cur_key, series or {})
            for arr in ser.values():
                for row in arr:
                    for j in range(3):
                        row[j] = round(row[j], 1)
            daily = {"d": dates, "s": {str(k): v for k, v in ser.items()}}
    con.close()

    return {
        "built": datetime.now().strftime("%d %b %Y"),
        "daily": daily,
        "lastDataDate": last_data_date,
        "provisional": provisional,
        "months": months,
        "amcs": amcs,
        "cats": [{"n": c[0], "g": c[1]} for c in cats],
        "schemes": schemes,          # [name, amcIdx, catIdx, benchmark?]
        "rows": data,                # [schemeIdx, monthIdx, flow, aum, aumPrev, navReturn]
        "perf": perf,                # [schemeIdx, monthIdx, r1y, r3y, r5y, b1y, b3y, b5y, aum]
    }


def render(payload, out_path, standalone=True):
    with open(TEMPLATE, encoding="utf-8") as f:
        tpl = f.read()
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    blob = blob.replace("</", "<\\/")         # never terminate the <script> early
    html = tpl.replace("/*__DATA__*/null", blob)
    amc_path = os.path.join(HERE, "data", "amc_financials.json")
    amc_blob = "null"
    if os.path.exists(amc_path):
        with open(amc_path, encoding="utf-8") as f:
            amc_blob = json.dumps(json.load(f), separators=(",", ":"), ensure_ascii=False).replace("</", "<\\/")
    html = html.replace("/*__AMC__*/null", amc_blob)
    idx_path = os.path.join(HERE, "data", "index_levels.json")
    idx_blob = "null"
    if os.path.exists(idx_path):
        with open(idx_path, encoding="utf-8") as f:
            idx_blob = json.dumps(json.load(f), separators=(",", ":")).replace("</", "<\\/")
    html = html.replace("/*__IDX__*/null", idx_blob)
    px_path = os.path.join(HERE, "data", "amc_prices.json")
    px_blob = "null"
    if os.path.exists(px_path):
        with open(px_path, encoding="utf-8") as f:
            px_blob = json.dumps(json.load(f), separators=(",", ":")).replace("</", "<\\/")
    html = html.replace("/*__PX__*/null", px_blob)
    if not standalone:
        # Artifact/embedded flavour: strip document wrapper, keep head-inner + body-inner
        head = html.split("<head>", 1)[1].split("</head>", 1)[0]
        body = html.split("<body", 1)[1].split(">", 1)[1].rsplit("</body>", 1)[0]
        html = head + body
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return len(html)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--embedded", action="store_true", help="body-only flavour for embedding")
    args = ap.parse_args()
    p = load_payload()
    n = render(p, args.out, standalone=not args.embedded)
    print(f"{args.out}: {n/1e6:.2f} MB | {len(p['months'])} months "
          f"({p['months'][0]} .. {p['months'][-1]}) | {len(p['amcs'])} AMCs | "
          f"{len(p['schemes'])} schemes | {len(p['rows'])} rows")
