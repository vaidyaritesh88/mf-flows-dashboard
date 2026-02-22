"""
AMC configuration — mapping of AMC names to AMFI IDs.
Top 10 AMCs by equity + hybrid AUM are flagged for dashboard focus.
"""

# Full AMC list (from AMFI fundperformancefilters endpoint, Feb 2026)
ALL_AMCS = {
    1: "360 ONE MF",
    2: "Aditya Birla Sun Life MF",
    3: "Angel One MF",
    4: "Axis MF",
    5: "Bajaj Finserv MF",
    6: "Bandhan MF",
    7: "Bank of India MF",
    8: "Baroda BNP Paribas MF",
    9: "Canara Robeco MF",
    10: "DSP MF",
    11: "Edelweiss MF",
    12: "Franklin Templeton MF",
    13: "Groww MF",
    14: "HDFC MF",
    15: "Helios MF",
    16: "HSBC MF",
    17: "ICICI Prudential MF",
    20: "Invesco MF",
    21: "ITI MF",
    22: "JM Financial MF",
    23: "Kotak Mahindra MF",
    24: "LIC MF",
    25: "Mahindra Manulife MF",
    26: "Mirae Asset MF",
    27: "Motilal Oswal MF",
    28: "Navi MF",
    29: "Nippon India MF",
    30: "NJ MF",
    31: "Old Bridge MF",
    32: "PGIM India MF",
    33: "PPFAS MF",
    34: "Quant MF",
    35: "Quantum MF",
    36: "SBI MF",
    37: "Samco MF",
    38: "Shriram MF",
    39: "Sundaram MF",
    40: "Tata MF",
    41: "Taurus MF",
    42: "Trust MF",
    43: "Union MF",
    44: "UTI MF",
    45: "WhiteOak Capital MF",
    46: "Zerodha MF",
}

# Top AMCs to highlight in the dashboard (will be refined with actual AUM data)
TOP_AMCS = [
    "SBI MF",
    "HDFC MF",
    "ICICI Prudential MF",
    "Nippon India MF",
    "Kotak Mahindra MF",
    "Axis MF",
    "UTI MF",
    "DSP MF",
    "Mirae Asset MF",
    "Tata MF",
    "Motilal Oswal MF",
    "Aditya Birla Sun Life MF",
]

# Short display names for charts
SHORT_NAMES = {
    "360 ONE MF": "360 ONE",
    "Aditya Birla Sun Life MF": "ABSL",
    "Axis MF": "Axis",
    "Bajaj Finserv MF": "Bajaj Finserv",
    "Bandhan MF": "Bandhan",
    "Baroda BNP Paribas MF": "Baroda BNP",
    "Canara Robeco MF": "Canara Robeco",
    "DSP MF": "DSP",
    "Edelweiss MF": "Edelweiss",
    "Franklin Templeton MF": "Franklin",
    "HDFC MF": "HDFC",
    "HSBC MF": "HSBC",
    "ICICI Prudential MF": "ICICI Pru",
    "Invesco MF": "Invesco",
    "Kotak Mahindra MF": "Kotak",
    "LIC MF": "LIC",
    "Mirae Asset MF": "Mirae",
    "Motilal Oswal MF": "Motilal Oswal",
    "Nippon India MF": "Nippon",
    "PPFAS MF": "PPFAS",
    "Quant MF": "Quant",
    "SBI MF": "SBI",
    "Sundaram MF": "Sundaram",
    "Tata MF": "Tata",
    "UTI MF": "UTI",
    "WhiteOak Capital MF": "WhiteOak",
}


def short_name(amc_name: str) -> str:
    """Return short display name for an AMC."""
    return SHORT_NAMES.get(amc_name, amc_name.replace(" Mutual Fund", "").replace(" MF", ""))
