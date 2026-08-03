"""
Fetch daily FII/DII (Foreign & Domestic Institutional Investor) trading activity
from NSE India, and append it to data/fii_dii_data.csv in this repo.

Designed to run both locally and inside GitHub Actions (see
.github/workflows/fetch-fii-dii.yml). The output path is relative to the repo
root, so it works the same in both environments as long as this script is run
from the repo root (e.g. `python scripts/fetch_fii_dii.py`).

Requirements:
    pip install curl_cffi pandas

Note: this uses curl_cffi instead of plain `requests`, because NSE's bot detection
fingerprints the TLS handshake itself — a plain `requests` session can send perfect
browser-like headers and still get blocked with a 403. curl_cffi impersonates a real
Chrome TLS fingerprint, which gets past it far more reliably.
"""

from curl_cffi import requests
import pandas as pd
import os
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# Path is relative to repo root so it works identically locally and in CI.
OUTPUT_FILE = os.path.join("data", "fii_dii_data.csv")
NSE_HOME_URL = "https://www.nseindia.com"
NSE_API_URL = "https://www.nseindia.com/api/fiidiiTradeReact"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "DNT": "1",
}

API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.nseindia.com/reports/fii-dii",
    "X-Requested-With": "XMLHttpRequest",
}


def fetch_fii_dii_data():
    """
    Fetches FII/DII data from NSE. Returns a DataFrame with columns:
    date, category, buy_value, sell_value, net_value
    """
    session = requests.Session(impersonate="chrome124")
    session.headers.update(HEADERS)

    # Step 1: warm up the session (grab cookies) by hitting the homepage first.
    resp = session.get(NSE_HOME_URL, timeout=15)
    resp.raise_for_status()

    time.sleep(1)

    # Step 2: call the actual API with the session's cookies attached.
    resp = session.get(NSE_API_URL, headers=API_HEADERS, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data)

    if df.empty:
        raise ValueError("NSE returned no FII/DII data — try again later or check the endpoint.")

    df.columns = [c.strip().lower() for c in df.columns]
    rename_map = {
        "category": "category",
        "date": "date",
        "buyvalue": "buy_value",
        "sellvalue": "sell_value",
        "netvalue": "net_value",
    }
    df = df.rename(columns=rename_map)

    df["date"] = pd.to_datetime(df["date"], dayfirst=True)
    for col in ["buy_value", "sell_value", "net_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["date", "category", "buy_value", "sell_value", "net_value"]].sort_values(
        ["date", "category"]
    ).reset_index(drop=True)


def _long_to_wide(long_df):
    """
    Converts long-format rows (one row per date+category) into wide format:
    one row per date, with DII columns followed by FII/FPI columns —
    matching the historical archive layout:
    date, category, buy_value, sell_value, net_value, category, buy_value, sell_value, net_value
    """
    dii = long_df[long_df["category"] == "DII"][
        ["date", "category", "buy_value", "sell_value", "net_value"]
    ]
    fii = long_df[long_df["category"] == "FII/FPI"][
        ["date", "category", "buy_value", "sell_value", "net_value"]
    ]
    dii.columns = ["date", "c1", "b1", "s1", "n1"]
    fii.columns = ["date", "c2", "b2", "s2", "n2"]

    wide = pd.merge(dii, fii, on="date", how="outer").sort_values("date").reset_index(drop=True)
    wide.columns = [
        "date", "category", "buy_value", "sell_value", "net_value",
        "category", "buy_value", "sell_value", "net_value",
    ]
    return wide


def _wide_to_long(wide_df):
    """Reverses _long_to_wide, for internal use when appending new data."""
    # wide_df has duplicate column names; access by position instead of name
    cols = wide_df.columns.tolist()
    dii = wide_df.iloc[:, [0, 1, 2, 3, 4]].copy()
    dii.columns = ["date", "category", "buy_value", "sell_value", "net_value"]
    fii = wide_df.iloc[:, [0, 5, 6, 7, 8]].copy()
    fii.columns = ["date", "category", "buy_value", "sell_value", "net_value"]
    long_df = pd.concat([dii, fii], ignore_index=True)
    long_df["date"] = pd.to_datetime(long_df["date"], dayfirst=True)
    return long_df.dropna(subset=["category"]).sort_values(["date", "category"]).reset_index(drop=True)


def append_to_archive(new_df, output_file=OUTPUT_FILE):
    """
    Appends new_df (long format) to the CSV archive, storing the archive itself
    in WIDE format (one row per date, DII then FII/FPI columns side by side),
    avoiding duplicate dates.
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if os.path.exists(output_file):
        existing_wide = pd.read_csv(output_file)
        existing_long = _wide_to_long(existing_wide)
        combined_long = pd.concat([existing_long, new_df], ignore_index=True)
        combined_long = combined_long.drop_duplicates(subset=["date", "category"], keep="last")
    else:
        combined_long = new_df

    combined_long = combined_long.sort_values(["date", "category"]).reset_index(drop=True)
    combined_wide = _long_to_wide(combined_long)

    # Write with explicit duplicate headers (pandas would otherwise suffix them)
    combined_wide_out = combined_wide.copy()
    combined_wide_out["date"] = pd.to_datetime(combined_wide_out["date"]).dt.strftime("%d-%m-%Y")
    header = "date,category,buy_value,sell_value,net_value,category,buy_value,sell_value,net_value\n"
    with open(output_file, "w") as f:
        f.write(header)
        for _, row in combined_wide_out.iterrows():
            f.write(",".join(str(v) for v in row.values) + "\n")

    return combined_long


def show_date(date_str, output_file=OUTPUT_FILE):
    """Look up FII/DII figures for a specific date from the local archive."""
    if not os.path.exists(output_file):
        print(f"No archive found yet at {output_file}.")
        return

    wide = pd.read_csv(output_file)
    long_df = _wide_to_long(wide)
    target = long_df[long_df["date"] == pd.to_datetime(date_str, dayfirst=True)]

    if target.empty:
        print(f"No data found for {date_str} in the archive.")
        print(f"Archive currently covers {long_df['date'].min().date()} to {long_df['date'].max().date()}.")
    else:
        print(f"FII/DII data for {date_str}:")
        print(target.to_string(index=False))


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "--date":
        show_date(sys.argv[2])
        return

    print(f"[{datetime.now()}] Fetching FII/DII data from NSE...")
    try:
        new_df = fetch_fii_dii_data()
    except Exception as e:
        print(f"ERROR fetching data: {e}")
        sys.exit(1)  # non-zero exit so the GitHub Action step is marked failed

    print(f"Fetched {len(new_df)} rows covering "
          f"{new_df['date'].min().date()} to {new_df['date'].max().date()}")

    combined = append_to_archive(new_df)
    print(f"Archive updated: {OUTPUT_FILE}  (total trading days now: {combined['date'].nunique()})")

    latest_date = combined["date"].max()
    latest = combined[combined["date"] == latest_date]
    print(f"\nLatest data ({latest_date.date()}):")
    print(latest.to_string(index=False))


if __name__ == "__main__":
    main()
