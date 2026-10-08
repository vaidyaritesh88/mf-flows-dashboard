"""
Fetch share prices for the listed AMCs (Yahoo Finance chart API) and save month-end closes
plus the latest close to data/amc_prices.json for the dashboard's Valuation tab.

Usage:  python fetch_amc_prices.py
Prices are split/bonus-adjusted (Yahoo 'close'), so pair them with the CURRENT share count.
"""
import json, os, datetime, sys
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data", "amc_prices.json")
TICKERS = {"IPRU": "ICICIAMC.NS", "HDFC": "HDFCAMC.NS", "NAM": "NAM-INDIA.NS", "SBI": "SBIFUNDS.NS"}
H = {"User-Agent": "Mozilla/5.0"}


def fetch(sym):
    p0 = int(datetime.datetime(2018, 1, 1).timestamp()); p1 = int(datetime.datetime.now().timestamp()) + 86400
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                     params={"period1": p0, "period2": p1, "interval": "1d"}, headers=H, timeout=60)
    r.raise_for_status()
    j = r.json()["chart"]["result"][0]
    rows = [(datetime.datetime.fromtimestamp(t, datetime.timezone.utc).date(), c)
            for t, c in zip(j["timestamp"], j["indicators"]["quote"][0]["close"]) if c]
    me = {}
    for d, c in rows:
        me[d.strftime("%Y-%m")] = round(c, 2)             # last trading day of each month wins
    last_d, last_c = rows[-1]
    return {"symbol": sym, "monthly": me, "last": round(last_c, 2), "last_date": last_d.isoformat(),
            "first_date": rows[0][0].isoformat()}


if __name__ == "__main__":
    out = {}
    for key, sym in TICKERS.items():
        try:
            out[key] = fetch(sym)
            print(f"{key:5} {sym:14} {out[key]['first_date']} .. {out[key]['last_date']}  last {out[key]['last']:,.2f}")
        except Exception as e:
            print(f"{key}: failed ({e})", file=sys.stderr)
    if not out:
        sys.exit(1)
    prev = {}
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            prev = json.load(f).get("prices", {})
    prev.update(out)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump({"fetched": datetime.date.today().isoformat(), "prices": prev}, f)
    print("wrote", OUT)
