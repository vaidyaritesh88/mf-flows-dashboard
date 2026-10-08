"""
Compile standardised listed-AMC financials from the saved models.
==================================================================
Reads the latest models in Janchor/BFSI/Financial Services/AMCs and writes
data/amc_financials.json in one common schema (Rs mn; AUM in Rs bn):

  rev      Revenue from operations          oi      Other income (incl. MTM gains)
  emp      Employee cost (incl. ESOP)        esop    ESOP cost where disclosed
  oth      Other operating costs (incl. fees & commission, scheme expenses)
  opex     emp + oth (excludes D&A and finance cost)
  ebitda   rev - opex                        da      Depreciation & amortisation
  ebit     ebitda - da                       fin     Finance cost
  pbt_x    ebit - fin  (PBT excl. other income)
  pbt      reported PBT (= pbt_x + oi, small rounding/associate differences kept)
  tax      tax expense                       pat     reported PAT
  pat_x    PAT excl. other income = pat - oi * (1 - tax/pbt)
  mf_fee   MF management fee revenue where disclosed
  aum      Total AAUM (MF + alternates)      aum_mf  MF AAUM (quarterly/annual average)
  aum_alt  PMS / AIF / advisory / offshore / managed accounts AAUM
  aum_eq   Equity-oriented MF AAUM (definition varies by company - see notes)
  aum_debt, aum_liq, aum_pass  Debt / liquid / passive-ETF-others MF AAUM

Usage:  python build_amc_data.py
Re-run after replacing any model file (update the MODEL paths below).
"""
import json, os, datetime, warnings
import openpyxl
warnings.filterwarnings("ignore")

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data", "amc_financials.json")
AMC_DIR = r"C:\Users\Ritesh Vaidya\OneDrive\Documents\Janchor\BFSI\Financial Services\AMCs"
MODEL = {
    "IPRU":   os.path.join(AMC_DIR, r"ICICI Pru AMC\Financial Model\Janchor Models\20260921_IPruAMC_Model.xlsx"),
    "HDFC":   os.path.join(AMC_DIR, r"HDFC AMC\Models\20260914_BERN_HDFCAMC.IN.xlsx"),
    "NAM":    os.path.join(AMC_DIR, r"Nippon AMC\Models\20260914_BERN_NAM.IN.xlsx"),
    "SBI":    os.path.join(AMC_DIR, r"SBI MF\20260914_SBI MF_BoFAML.xlsx"),
    "SBI_K":  os.path.join(AMC_DIR, r"SBI MF\[Kotak] SBI Funds Management_historical.xlsx"),
    "CRAMC":  os.path.join(AMC_DIR, r"Canara Robeco\20260914_Canara Robeco AMC.xlsx"),
    "MOSL":   os.path.join(AMC_DIR, r"SBI MF\20260421_AMCs comparison_MOSL.xlsx"),
}


def sheet(path, name):
    """Return {row_number: [values...]} (1-based rows, 0-based list -> col A = index 0)."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[name]
    return {i: list(r) for i, r in enumerate(ws.iter_rows(values_only=True), 1)}


def num(v):
    if v is None or isinstance(v, str):
        return None
    try:
        f = float(v)
    except Exception:
        return None
    return None if f != f else f          # NaN guard


def cell(S, r, c):
    """1-based row, 1-based column."""
    row = S.get(r)
    if not row or c - 1 >= len(row):
        return None
    return num(row[c - 1])


def sub(a, b):
    return None if a is None or b is None else a - b


def add(*xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) if xs else None


def finalize(p):
    """Fill derived lines from the raw inputs already in p."""
    p["opex"] = add(p.get("emp"), p.get("oth"))
    # fees & commission paid (distribution cost on PMS/AIF/advisory; disclosed by IPRU and Nippon)
    comm = p.get("comm")
    p["oth_x"] = sub(p.get("oth"), comm) if comm is not None else p.get("oth")
    p["opex_x"] = sub(p["opex"], comm) if comm is not None else p["opex"]
    p["rev_net"] = sub(p.get("rev"), comm) if comm is not None else p.get("rev")
    p["ebitda"] = sub(p.get("rev"), p["opex"])
    p["ebit"] = sub(p["ebitda"], p.get("da"))
    p["pbt_x"] = sub(p["ebit"], p.get("fin") or 0) if p["ebit"] is not None else None
    if p.get("pbt") is None and p["pbt_x"] is not None:
        p["pbt"] = add(p["pbt_x"], p.get("oi"))
    if p.get("pat") is not None and p.get("oi") is not None and p.get("pbt") and p.get("tax") is not None:
        etr = p["tax"] / p["pbt"] if p["pbt"] else 0.25
        p["pat_x"] = p["pat"] - p["oi"] * (1 - etr)
    else:
        p["pat_x"] = None
    if p.get("aum") is None:
        # total only when both parts are known (alternates may be undisclosed for a quarter)
        p["aum"] = (p["aum_mf"] + p["aum_alt"]) if p.get("aum_mf") is not None and p.get("aum_alt") is not None else None
    # aum_lowm = EPFO / pension-type mandates that earn almost nothing; aum_x = total excluding them
    if p.get("aum_lowm") is None:
        p["aum_lowm"] = 0.0 if p.get("aum_alt") is not None else None
    p["aum_x"] = (p["aum"] - p["aum_lowm"]) if p.get("aum") is not None and p.get("aum_lowm") is not None else None
    # round
    for k, v in list(p.items()):
        if isinstance(v, float):
            p[k] = round(v, 1)
    return p


def period_label_q(fy, q):
    return f"{q}QFY{fy % 100:02d}"


# ───────────────────────────── ICICI Pru AMC ─────────────────────────────
def find_row(S, label, after=0, exact=False):
    """Row number whose column-B label matches (case-insensitive, stripped); first match after `after`."""
    lab = label.strip().lower()
    for r in sorted(S):
        if r <= after:
            continue
        for v in S[r][:2]:                                   # label may sit in column A or B
            if isinstance(v, str):
                t = v.strip().lower()
                if (t == lab) if exact else t.startswith(lab):
                    return r
    raise KeyError(f"label not found: {label!r} after row {after}")


def build_ipru():
    # The Janchor model is edited by hand, so every row is located by its label, not a fixed number.
    Q = sheet(MODEL["IPRU"], "Quarterly")
    IS = sheet(MODEL["IPRU"], "IS")
    q = lambda lab, after=0, exact=False: find_row(Q, lab, after, exact)
    r_rev, r_oi, r_emp, r_fin, r_oth = q("Revenue from operations"), q("Other income"), q("Staff costs"), q("Finance costs"), q("Other expenses")
    r_da, r_pbt = q("Depreciation and amortization"), q("PBT", exact=True)
    r_tax, r_pat = q("Tax", r_pbt, exact=True), q("PAT", r_pbt, exact=True)
    r_comm, r_esop = q("Fees and commission expense"), q("ESOP expense")
    r_mfq = q("ICICI Pru MF QAAAUM (INR Bn)")
    r_aeq, r_hyb, r_debt, r_liq, r_pass, r_arb = (q(l, r_mfq, exact=True) for l in ("Active Equity", "Equity Hybrid", "Debt", "Liquid", "Passive", "Arbitrage"))
    r_qaum = q("QAAUM (Rs bn)", exact=True)
    r_mf, r_alt, r_tot = q("Total MF QAAUM", r_qaum), q("Alternates (incl Advisory)", r_qaum), q("Overall QAAUM", r_qaum)
    hdr = Q[find_row(Q, "All figures in")]                   # period labels e.g. 3Q25 .. 1Q27 from column C
    quarterly = []
    for c in range(3, len(hdr) + 1):
        lab = hdr[c - 1]
        if not isinstance(lab, str) or not lab.strip() or cell(Q, r_rev, c) is None:
            continue
        qn, fy = int(lab[0]), 2000 + int(lab[2:4])
        p = dict(p=period_label_q(fy, qn), rev=cell(Q, r_rev, c), oi=cell(Q, r_oi, c), emp=cell(Q, r_emp, c),
                 esop=cell(Q, r_esop, c), fin=cell(Q, r_fin, c), oth=cell(Q, r_oth, c), da=cell(Q, r_da, c),
                 comm=cell(Q, r_comm, c), pbt=cell(Q, r_pbt, c), tax=cell(Q, r_tax, c), pat=cell(Q, r_pat, c),
                 aum=cell(Q, r_tot, c), aum_mf=cell(Q, r_mf, c), aum_alt=cell(Q, r_alt, c),
                 aum_eq=add(cell(Q, r_aeq, c), cell(Q, r_hyb, c)), aum_active_eq=cell(Q, r_aeq, c),
                 aum_debt=cell(Q, r_debt, c), aum_liq=cell(Q, r_liq, c),
                 aum_pass=add(cell(Q, r_pass, c), cell(Q, r_arb, c)), mf_fee=None)
        quarterly.append(finalize(p))
    i_ = lambda lab, after=0, exact=False: find_row(IS, lab, after, exact)
    i_aum, i_split = i_("AAUM (Annual Average)"), i_("AAUM split")
    i_mf, i_pms, i_aif, i_adv = i_("MF", i_split, True), i_("PMS", i_split, True), i_("AIF", i_split, True), i_("Advisory Assets", i_split)
    i_rev, i_mffee = i_("Asset management fees"), i_("Investment management fees")
    i_emp, i_comm, i_oth = i_("Employee expense"), i_("Fees and commission expense"), i_("Other expenditure")
    i_da, i_fin, i_oi = i_("Depreciation and amortization"), i_("Finance cost"), i_("Other income")
    i_pbt = i_("PBT", i_oi, True); i_tax = i_("Tax", i_pbt, True); i_pat = i_("PAT", i_pbt, True)
    hdr = IS[find_row(IS, "All figures")]                     # Mar17 .. Mar36 datetimes from column C
    annual, forecast = [], []
    for c in range(3, len(hdr) + 1):
        d = hdr[c - 1]
        if not isinstance(d, datetime.datetime) or cell(IS, i_rev, c) is None:
            continue
        fy = d.year
        est = fy >= 2027
        if est and fy > 2031:
            continue
        p = dict(p=f"FY{fy % 100:02d}" + ("E" if est else ""), rev=cell(IS, i_rev, c), oi=cell(IS, i_oi, c),
                 emp=cell(IS, i_emp, c), esop=None, oth=add(cell(IS, i_comm, c), cell(IS, i_oth, c)), comm=cell(IS, i_comm, c),
                 da=cell(IS, i_da, c), fin=cell(IS, i_fin, c), pbt=cell(IS, i_pbt, c), tax=cell(IS, i_tax, c), pat=cell(IS, i_pat, c),
                 mf_fee=cell(IS, i_mffee, c),
                 aum=cell(IS, i_aum, c) if fy < 2023 else cell(IS, i_split, c), aum_lowm=0.0,
                 aum_mf=cell(IS, i_mf, c), aum_alt=add(cell(IS, i_pms, c), cell(IS, i_aif, c), cell(IS, i_adv, c)))
        qs = [x for x in quarterly if x["p"].endswith(f"FY{fy % 100:02d}") and x.get("aum_eq")]
        if len(qs) == 4:                                       # annual MF mix = average of the four quarters where all exist
            for k in ("aum_eq", "aum_active_eq", "aum_debt", "aum_liq", "aum_pass"):
                p[k] = sum(x[k] for x in qs) / 4
        (forecast if est else annual).append(finalize(p))
    r_sh = q("Shares outstanding")
    shares = [cell(Q, r_sh, c) for c in range(3, len(hdr) + 1) if cell(Q, r_sh, c)]
    return dict(key="IPRU", name="ICICI Pru AMC", ticker="ICICIAMC IN", shares=shares[-1] if shares else None,
                source="Janchor model (20260921_IPruAMC_Model.xlsx)",
                notes="Quarterly from 3QFY25 (IPO disclosures). Equity-oriented AAUM = active equity + equity hybrid, ex arbitrage. "
                      "Alternates = PMS + AIF + advisory; fees & commission paid on that book are shown separately. "
                      "Forecast years are the Janchor model's projections; the model's FY26 annual P&L runs ~0.7% below the sum of its four quarters.",
                quarterly=quarterly, annual=annual, forecast=forecast)


# ───────────────────────────── HDFC AMC ─────────────────────────────
def build_hdfc():
    S = sheet(MODEL["HDFC"], "Consol Co")
    def rec(c, label):
        da = cell(S, 17, c)
        p = dict(p=label, rev=cell(S, 10, c), oi=cell(S, 11, c), emp=cell(S, 16, c), esop=cell(S, 33, c),
                 oth=cell(S, 18, c), da=da, fin=None, pbt=cell(S, 20, c), tax=cell(S, 21, c), pat=cell(S, 22, c),
                 mf_fee=cell(S, 8, c), aum_mf=cell(S, 70, c), aum_alt=cell(S, 147, c),
                 aum_eq=cell(S, 71, c), aum_active_eq=cell(S, 141, c), aum_debt=cell(S, 72, c),
                 aum_liq=cell(S, 73, c), aum_pass=cell(S, 74, c))
        return finalize(p)
    annual = [rec(3 + i, f"FY{18 + i}") for i in range(9)]                     # FY18..FY26
    quarterly = []
    for i in range(37):                                                          # 1Q18 .. 1Q27
        c = 20 + i
        fy, q = 2018 + i // 4, i % 4 + 1
        quarterly.append(rec(c, period_label_q(fy, q)))
    sh = [cell(S, 30, c) for c in range(20, 57) if cell(S, 30, c)]           # diluted weighted shares, latest quarter (post 1:1 bonus)
    return dict(key="HDFC", name="HDFC AMC", ticker="HDFCAMC IN", shares=sh[-1] if sh else None,
                source="Bernstein model (20260914_BERN_HDFCAMC.IN.xlsx)",
                notes="Equity-oriented AAUM as reported by HDFC AMC (equity incl. hybrid). Alternates = PMS/SMA AUM. "
                      "Finance cost not separately disclosed (inside other costs).",
                quarterly=quarterly, annual=annual, forecast=[])


# ───────────────────────────── Nippon Life India AMC ─────────────────────────────
def build_nam():
    S = sheet(MODEL["NAM"], "Qtrly")
    def rec(c, label):
        p = dict(p=label, rev=cell(S, 8, c), oi=cell(S, 12, c), emp=cell(S, 17, c), esop=cell(S, 18, c),
                 oth=add(cell(S, 23, c), cell(S, 24, c)), comm=cell(S, 23, c), da=cell(S, 20, c), fin=cell(S, 22, c),
                 pbt=cell(S, 26, c), tax=cell(S, 27, c), pat=cell(S, 28, c),
                 mf_fee=cell(S, 9, c), aum_mf=cell(S, 88, c), aum_alt=cell(S, 96, c),
                 aum_eq=cell(S, 89, c), aum_active_eq=None, aum_debt=cell(S, 90, c),
                 aum_liq=cell(S, 91, c), aum_pass=cell(S, 92, c))
        # managed accounts (EPFO / CMPFO / pension mandates) = low-margin; offshore stays in
        ma, off, tot = cell(S, 94, c), cell(S, 95, c), cell(S, 96, c)
        if ma is None and tot is not None and off is not None:
            ma = tot - off
        p["aum_lowm"] = ma
        return finalize(p)
    annual = [rec(3 + i, f"FY{18 + i}") for i in range(9)]
    quarterly = [rec(20 + i, period_label_q(2018 + i // 4, i % 4 + 1)) for i in range(37)]
    sh = [cell(S, 39, c) for c in range(20, 57) if cell(S, 39, c)]
    return dict(key="NAM", name="Nippon Life India AMC", ticker="NAM IN", shares=sh[-1] if sh else None,
                source="Bernstein model (20260914_BERN_NAM.IN.xlsx)",
                notes="Equity-oriented AAUM = reported equity AAUM (ETFs shown under passive). "
                      "Alternates = managed accounts + offshore; managed accounts (EPFO / CMPFO / pension mandates) are treated as "
                      "low-margin and excluded from the default AAUM base, offshore stays in.",
                quarterly=quarterly, annual=annual, forecast=[])


# ───────────────────────────── SBI Funds Management ─────────────────────────────
def build_sbi():
    S = sheet(MODEL["SBI"], "Model")
    K = sheet(MODEL["SBI_K"], "Earnmodel")
    KA = sheet(MODEL["SBI_K"], "AUM_breakup")
    def rec(c, label):
        p = dict(p=label, rev=cell(S, 5, c), oi=add(cell(S, 7, c), cell(S, 8, c)), emp=cell(S, 14, c),
                 esop=cell(S, 16, c), oth=add(cell(S, 21, c), cell(S, 23, c), cell(S, 27, c)),
                 da=cell(S, 29, c), fin=cell(S, 28, c), pbt=cell(S, 33, c), tax=cell(S, 36, c), pat=cell(S, 38, c),
                 mf_fee=cell(S, 136, c), aum_mf=cell(S, 182, c), aum_alt=add(cell(S, 200, c), cell(S, 203, c)),
                 aum_eq=cell(S, 174, c), aum_active_eq=None, aum_debt=cell(S, 176, c),
                 aum_liq=cell(S, 178, c), aum_pass=cell(S, 180, c),
                 aum_lowm=cell(S, 201, c))          # PMS & advisory = overwhelmingly the EPFO mandate
        return finalize(p)
    annual = [rec(3 + i, f"FY{17 + i}") for i in range(10)]                    # FY17..FY26
    # BofA's ESOP line is 10x the disclosed figure for FY25/FY26 (Kotak: FY25 = 287.3); use Kotak for FY25, blank FY26
    for x in annual:
        if x["p"] == "FY25":
            x["esop"] = cell(K, 67, 5)
        elif x["p"] == "FY26":
            x["esop"] = None
    # quarterly: BofA columns S..AA = 1Q25..1Q27 (col 19..27); P&L only where populated
    quarterly = []
    for i in range(9):
        c = 19 + i
        fy, q = 2025 + i // 4, i % 4 + 1
        quarterly.append(rec(c, period_label_q(fy, q)))
    # 4QFY25 = FY25 - 9MFY25 from the Kotak historical file (Earnmodel cols: C=FY23 D=FY24 E=FY25 G=9MFY25 H=9MFY26)
    def kq(r):
        return sub(cell(K, r, 5), cell(K, r, 7))
    q4 = dict(rev=kq(45), oi=kq(49), emp=kq(66), esop=kq(67),
              oth=sub(sub(kq(70), kq(78)), kq(80)), da=kq(80), fin=kq(78),
              pbt=kq(82), tax=kq(84), pat=kq(89), mf_fee=kq(46))
    # Kotak AUM_breakup holds quarter-end QAAUM: col D = 4QFY25, G = 3QFY25 (Dec-24), H = 3QFY26 (Dec-25)
    alt_fill = {"3QFY25": add(cell(KA, 14, 7), cell(KA, 15, 7)),
                "4QFY25": add(cell(KA, 14, 4), cell(KA, 15, 4)),
                "3QFY26": add(cell(KA, 14, 8), cell(KA, 15, 8))}
    pms_fill = {"3QFY25": cell(KA, 12, 7), "4QFY25": cell(KA, 12, 4), "3QFY26": cell(KA, 12, 8)}
    for x in quarterly:
        if x["p"] == "4QFY25":
            for k, v in q4.items():
                if x.get(k) is None:
                    x[k] = v
        if x["p"] in alt_fill and x.get("aum_alt") is None:
            x["aum_alt"] = alt_fill[x["p"]]
            x["aum_lowm"] = pms_fill[x["p"]]
        x.pop("aum", None); x.pop("aum_x", None)
        finalize(x)
    # SBI's own quarterly alternates only disclosed post-listing; total AUM stays blank where unknown
    sh = [cell(S, 48, c) for c in range(19, 28) if cell(S, 48, c)]           # diluted weighted shares, latest actual quarter
    return dict(key="SBI", name="SBI Funds Management", ticker="SBIFUNDS IN", shares=sh[-1] if sh else None,
                source="BofA model (20260914_SBI MF_BoFAML.xlsx); 4QFY25 derived from Kotak historical (FY25 less 9MFY25)",
                notes="Quarterly P&L only for 4QFY25, 1QFY26, 4QFY26 and 1QFY27 (pre-listing quarters not disclosed). "
                      "Alternates = PMS + AIF + offshore; the PMS & advisory book (overwhelmingly the EPFO mandate) is treated as "
                      "low-margin and excluded from the default AAUM base. Equity-oriented AAUM as per BofA classification.",
                quarterly=quarterly, annual=annual, forecast=[])


# ───────────────────────────── Canara Robeco AMC ─────────────────────────────
def build_cramc():
    P = sheet(MODEL["CRAMC"], "XN-PL")
    X = sheet(MODEL["CRAMC"], "XN")
    M = sheet(MODEL["MOSL"], "Annual")
    annual = []
    for i, fy in enumerate([2024, 2025, 2026]):
        c = 2 + i                                   # B=FY24A, C=FY25A, D=FY26A
        rev, tot = cell(P, 3, c), cell(P, 4, c)
        da = {2025: cell(X, 21, 2), 2026: cell(X, 21, 3)}.get(fy)
        opex_tot = cell(P, 7, c)
        emp = cell(P, 5, c)
        oth_all = cell(P, 6, c)
        if da is None and opex_tot is not None and emp is not None and oth_all is not None:
            da = opex_tot - emp - oth_all           # FY24: balancing figure in the model's total
        p = dict(p=f"FY{fy % 100:02d}", rev=rev, oi=sub(tot, rev), emp=emp, esop=None, oth=oth_all, da=da, fin=None,
                 pbt=cell(P, 9, c), tax=sub(cell(P, 9, c), cell(P, 10, c)), pat=cell(P, 10, c), mf_fee=rev,
                 aum_mf=None, aum_alt=0.0, aum_eq=None)
        # AAUM from the MOSL comparison sheet (row 8 total, row 18 equity; cols B.. = FY18..)
        mc = 2 + (fy - 2018)
        if fy <= 2025:
            p["aum_mf"] = cell(M, 8, mc); p["aum_eq"] = cell(M, 18, mc)
        annual.append(finalize(p))
    return dict(key="CRAMC", name="Canara Robeco AMC", ticker="CRAMC IN",
                source="Canara Robeco model (20260914_Canara Robeco AMC.xlsx); AAUM from MOSL AMC comparison (Apr 2026)",
                notes="Annual only (FY24-FY26); FY26 AAUM not yet in the saved files, so FY26 yields are blank.",
                quarterly=[], annual=annual, forecast=[])


if __name__ == "__main__":
    data = dict(built=datetime.date.today().strftime("%d %b %Y"),
                companies=[build_ipru(), build_hdfc(), build_nam(), build_sbi()])
    for c in data["companies"]:                       # annual history shown from FY21
        c["annual"] = [x for x in c["annual"] if int(x["p"][2:4]) >= 21]
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    for c in data["companies"]:
        q = [x["p"] for x in c["quarterly"] if x.get("rev")]
        a = [x["p"] for x in c["annual"] if x.get("rev")]
        print(f"{c['key']:6} quarterly {len(q):2} ({q[0] if q else '-'}..{q[-1] if q else '-'}) | "
              f"annual {len(a):2} ({a[0] if a else '-'}..{a[-1] if a else '-'}) | forecast {len(c['forecast'])}")
    print("wrote", OUT, os.path.getsize(OUT) // 1024, "KB")
