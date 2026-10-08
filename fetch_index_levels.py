"""
Fetch month-end closes for the reference indices (Yahoo Finance chart API) and save
them to data/index_levels.json for the dashboard's AUM-bridge tab.

Usage:  python fetch_index_levels.py
Safe to re-run; on any network error the previous file is left untouched.
"""
import json, os, datetime, sys
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data", "index_levels.json")
INDICES = {                      # display name -> Yahoo symbol
    "Nifty 50": "^NSEI",
    "Nifty 500": "^CRSLDX",
    "Nifty Midcap 150": "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMLCAP250.NS",
}
H = {"User-Agent": "Mozilla/5.0"}


def month_end_closes(sym, start=datetime.date(2019, 1, 1)):
    p0 = int(datetime.datetime(start.year, start.month, start.day).timestamp())
    p1 = int(datetime.datetime.now().timestamp()) + 86400
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                     params={"period1": p0, "period2": p1, "interval": "1d"}, headers=H, timeout=60)
    r.raise_for_status()
    j = r.json()["chart"]["result"][0]
    closes = j["indicators"]["quote"][0]["close"]
    out = {}
    for t, c in zip(j["timestamp"], closes):
        if c is None:
            continue
        d = datetime.datetime.fromtimestamp(t, datetime.timezone.utc).date()
        out[d.strftime("%Y-%m")] = (d.isoformat(), round(c, 2))      # last trading day of each month wins
    return out


if __name__ == "__main__":
    series = {}
    for name, sym in INDICES.items():
        try:
            series[name] = month_end_closes(sym)
            print(f"{name:20} {len(series[name])} months, latest {max(series[name])} = {series[name][max(series[name])][1]:,.0f}")
        except Exception as e:
            print(f"{name}: failed ({e}); keeping previous data if any", file=sys.stderr)
    if not series:
        sys.exit(1)
    prev = {}
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            prev = json.load(f).get("series", {})
    prev.update(series)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"fetched": datetime.date.today().isoformat(), "series": prev}, f)
    print("wrote", OUT)
