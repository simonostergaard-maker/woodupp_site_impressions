"""
Preprocesses WoodUpp GSC data into a standalone HTML dashboard.
Reads the daily-updated CSV, merges with frozen historical JSON data,
and generates a self-contained index.html with all data embedded.

Usage:
    python preprocess.py                          # Use default CSV path
    python preprocess.py /path/to/csv_file.csv    # Custom CSV path
"""
import pandas as pd
import json
import re
import sys
import os
import heapq
import itertools
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
HISTORICAL_DIR = DATA_DIR / "historical"
TEMPLATE_HTML = SCRIPT_DIR / "template.html"
OUTPUT_HTML = SCRIPT_DIR / "index.html"

# Default CSV path (Windows) — override via command-line argument
DEFAULT_CSV = Path(r"C:\Users\sos\Desktop\Claude\Woodupp\woodupp_url_impressions.csv")

# Map site_url / country_code to readable market names
DOMAIN_MARKET_MAP = {
    "ae": {"domain": "woodupp.ae", "market": "UAE", "flag": "\U0001f1e6\U0001f1ea"},
    "at": {"domain": "woodupp.at", "market": "Austria", "flag": "\U0001f1e6\U0001f1f9"},
    "au": {"domain": "woodupp.au", "market": "Australia", "flag": "\U0001f1e6\U0001f1fa"},
    "be": {"domain": "woodupp.be", "market": "Belgium", "flag": "\U0001f1e7\U0001f1ea"},
    "ch": {"domain": "woodupp.ch", "market": "Switzerland", "flag": "\U0001f1e8\U0001f1ed"},
    "couk": {"domain": "woodupp.co.uk", "market": "United Kingdom", "flag": "\U0001f1ec\U0001f1e7"},
    "coza": {"domain": "woodupp.co.za", "market": "South Africa", "flag": "\U0001f1ff\U0001f1e6"},
    "com_na": {"domain": "woodupp.com.na", "market": "Namibia", "flag": "\U0001f1f3\U0001f1e6"},
    "com": {"domain": "woodupp.com", "market": "Global (.com)", "flag": "\U0001f310"},
    "us": {"domain": "woodupp.com/us", "market": "USA", "flag": "\U0001f1fa\U0001f1f8"},
    "de": {"domain": "woodupp.de", "market": "Germany", "flag": "\U0001f1e9\U0001f1ea"},
    "dk": {"domain": "woodupp.dk", "market": "Denmark", "flag": "\U0001f1e9\U0001f1f0"},
    "es": {"domain": "woodupp.es", "market": "Spain", "flag": "\U0001f1ea\U0001f1f8"},
    "eu": {"domain": "woodupp.eu", "market": "Europe (.eu)", "flag": "\U0001f1ea\U0001f1fa"},
    "fr": {"domain": "woodupp.fr", "market": "France", "flag": "\U0001f1eb\U0001f1f7"},
    "it": {"domain": "woodupp.it", "market": "Italy", "flag": "\U0001f1ee\U0001f1f9"},
    "nl": {"domain": "woodupp.nl", "market": "Netherlands", "flag": "\U0001f1f3\U0001f1f1"},
    "no": {"domain": "woodupp.no", "market": "Norway", "flag": "\U0001f1f3\U0001f1f4"},
    "pl": {"domain": "woodupp.pl", "market": "Poland", "flag": "\U0001f1f5\U0001f1f1"},
    "pt": {"domain": "woodupp.pt", "market": "Portugal", "flag": "\U0001f1f5\U0001f1f9"},
    "se": {"domain": "woodupp.se", "market": "Sweden", "flag": "\U0001f1f8\U0001f1ea"},
}

# ISO 3-letter to readable country name for visitor countries
COUNTRY_ISO_MAP = {
    "usa": "United States", "deu": "Germany", "fra": "France", "gbr": "United Kingdom",
    "nld": "Netherlands", "dnk": "Denmark", "bel": "Belgium", "che": "Switzerland",
    "can": "Canada", "aut": "Austria", "swe": "Sweden", "esp": "Spain",
    "zaf": "South Africa", "ita": "Italy", "aus": "Australia", "nor": "Norway",
    "ind": "India", "pol": "Poland", "irl": "Ireland", "prt": "Portugal",
    "are": "UAE", "bra": "Brazil", "mex": "Mexico", "jpn": "Japan",
    "kor": "South Korea", "chn": "China", "sgp": "Singapore", "hkg": "Hong Kong",
    "nzl": "New Zealand", "fin": "Finland", "rou": "Romania", "hun": "Hungary",
    "cze": "Czech Republic", "bgr": "Bulgaria", "hrv": "Croatia", "svk": "Slovakia",
    "svn": "Slovenia", "ltu": "Lithuania", "lva": "Latvia", "est": "Estonia",
    "grc": "Greece", "tur": "Turkey", "isr": "Israel", "sau": "Saudi Arabia",
    "kwt": "Kuwait", "qat": "Qatar", "bhr": "Bahrain", "omn": "Oman",
    "mys": "Malaysia", "tha": "Thailand", "phl": "Philippines", "idn": "Indonesia",
    "vnm": "Vietnam", "twn": "Taiwan", "col": "Colombia", "arg": "Argentina",
    "chl": "Chile", "per": "Peru", "nga": "Nigeria", "ken": "Kenya",
    "gha": "Ghana", "egy": "Egypt", "mar": "Morocco", "tun": "Tunisia",
    "lux": "Luxembourg", "mlt": "Malta", "cyp": "Cyprus", "isl": "Iceland",
    "nam": "Namibia", "pak": "Pakistan", "lka": "Sri Lanka", "bgd": "Bangladesh",
    "ukr": "Ukraine", "rus": "Russia", "blr": "Belarus", "srb": "Serbia",
    "bih": "Bosnia", "mkd": "North Macedonia", "alb": "Albania", "mne": "Montenegro",
    "geo": "Georgia", "arm": "Armenia", "aze": "Azerbaijan", "kaz": "Kazakhstan",
    "uzb": "Uzbekistan", "xkk": "Kosovo",
}


# ─── CSV Loading & Cleaning ───

def load_and_clean(csv_path):
    print(f"Loading CSV from {csv_path}...")
    df = pd.read_csv(csv_path, dtype={
        "is_anonymized_query": str,
        "is_anonymized_discover": str,
        "impressions": int,
        "clicks": int,
    })
    print(f"  Loaded {len(df):,} rows")

    df["data_date"] = pd.to_datetime(df["data_date"]).dt.strftime("%Y-%m-%d")
    df["is_anonymized_query"] = df["is_anonymized_query"].str.lower() == "true"
    df["is_anonymized_discover"] = df["is_anonymized_discover"].str.lower() == "true"

    df["market"] = df["country_code"].map(lambda x: DOMAIN_MARKET_MAP.get(x, {}).get("market", x))
    df["domain"] = df["country_code"].map(lambda x: DOMAIN_MARKET_MAP.get(x, {}).get("domain", x))
    df["visitor_country"] = df["country"].map(lambda x: COUNTRY_ISO_MAP.get(x, x))

    df["avg_position"] = (df["sum_position"] / df["impressions"]).round(1)
    df.loc[df["impressions"] == 0, "avg_position"] = 0

    df["ctr"] = (df["clicks"] / df["impressions"] * 100).round(2)
    df.loc[df["impressions"] == 0, "ctr"] = 0

    df["url_path"] = df["url"].apply(lambda u: "/" + "/".join(u.split("/")[3:]) if isinstance(u, str) and len(u.split("/")) > 3 else "/")

    print(f"  Date range: {df['data_date'].min()} to {df['data_date'].max()}")
    print(f"  Markets: {df['market'].nunique()}")
    return df


# ─── Data Generation Functions ───

def generate_overview(df):
    dates = sorted(df["data_date"].unique().tolist())
    markets = sorted(df["market"].unique().tolist())

    overview = {
        "dates": dates,
        "markets": [{
            "code": code,
            "market": info["market"],
            "domain": info["domain"],
            "flag": info["flag"],
        } for code, info in sorted(DOMAIN_MARKET_MAP.items(), key=lambda x: x[1]["market"])],
        "totals": {
            "impressions": int(df["impressions"].sum()),
            "clicks": int(df["clicks"].sum()),
            "queries": int((~df["is_anonymized_query"]).sum()),
            "anonymized_queries": int(df["is_anonymized_query"].sum()),
            "unique_urls": int(df["url"].nunique()),
            "unique_keywords": int(df.loc[~df["is_anonymized_query"] & df["query"].notna(), "query"].nunique()),
            "avg_ctr": round(float(df["clicks"].sum() / df["impressions"].sum() * 100), 2) if df["impressions"].sum() > 0 else 0,
            "avg_position": round(float(df["sum_position"].sum() / df["impressions"].sum()), 1) if df["impressions"].sum() > 0 else 0,
        },
        "per_market": {},
    }

    for market in markets:
        mdf = df[df["market"] == market]
        overview["per_market"][market] = {
            "impressions": int(mdf["impressions"].sum()),
            "clicks": int(mdf["clicks"].sum()),
            "rows": int(len(mdf)),
            "unique_urls": int(mdf["url"].nunique()),
            "unique_keywords": int(mdf.loc[~mdf["is_anonymized_query"] & mdf["query"].notna(), "query"].nunique()),
            "avg_ctr": round(float(mdf["clicks"].sum() / mdf["impressions"].sum() * 100), 2) if mdf["impressions"].sum() > 0 else 0,
            "avg_position": round(float(mdf["sum_position"].sum() / mdf["impressions"].sum()), 1) if mdf["impressions"].sum() > 0 else 0,
        }

    return overview


def generate_daily_metrics(df):
    daily = df.groupby(["data_date", "market"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        total_rows=("impressions", "count"),
        anon_queries=("is_anonymized_query", "sum"),
    ).reset_index()

    daily["avg_position"] = (daily["sum_position"] / daily["impressions"]).round(1)
    daily["ctr"] = (daily["clicks"] / daily["impressions"] * 100).round(2)
    daily["anon_pct"] = (daily["anon_queries"] / daily["total_rows"] * 100).round(1)

    result = {}
    for _, row in daily.iterrows():
        date = row["data_date"]
        if date not in result:
            result[date] = {}
        result[date][row["market"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
            "total_rows": int(row["total_rows"]),
            "anon_queries": int(row["anon_queries"]),
            "anon_pct": float(row["anon_pct"]) if pd.notna(row["anon_pct"]) else 0,
        }

    daily_all = df.groupby("data_date").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        total_rows=("impressions", "count"),
        anon_queries=("is_anonymized_query", "sum"),
    ).reset_index()
    daily_all["avg_position"] = (daily_all["sum_position"] / daily_all["impressions"]).round(1)
    daily_all["ctr"] = (daily_all["clicks"] / daily_all["impressions"] * 100).round(2)
    daily_all["anon_pct"] = (daily_all["anon_queries"] / daily_all["total_rows"] * 100).round(1)

    for _, row in daily_all.iterrows():
        date = row["data_date"]
        if date not in result:
            result[date] = {}
        result[date]["All Markets"] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
            "total_rows": int(row["total_rows"]),
            "anon_queries": int(row["anon_queries"]),
            "anon_pct": float(row["anon_pct"]) if pd.notna(row["anon_pct"]) else 0,
        }

    return result


def generate_anonymized_data(df):
    anon_by_market_date = df.groupby(["data_date", "market", "is_anonymized_query"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        count=("impressions", "count"),
    ).reset_index()

    result = {"by_market_date": {}, "by_country": {}, "by_device": {}, "by_search_type": {}}

    for _, row in anon_by_market_date.iterrows():
        key = f"{row['data_date']}|{row['market']}"
        if key not in result["by_market_date"]:
            result["by_market_date"][key] = {"anon": {}, "known": {}}
        bucket = "anon" if row["is_anonymized_query"] else "known"
        result["by_market_date"][key][bucket] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "count": int(row["count"]),
        }

    anon_country = df.groupby(["visitor_country", "is_anonymized_query"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        count=("impressions", "count"),
    ).reset_index()

    for _, row in anon_country.iterrows():
        vc = row["visitor_country"]
        if vc not in result["by_country"]:
            result["by_country"][vc] = {"anon": {"count": 0, "impressions": 0}, "known": {"count": 0, "impressions": 0}}
        bucket = "anon" if row["is_anonymized_query"] else "known"
        result["by_country"][vc][bucket] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "count": int(row["count"]),
        }

    anon_device = df.groupby(["device", "is_anonymized_query"]).agg(
        count=("impressions", "count"),
        impressions=("impressions", "sum"),
    ).reset_index()
    for _, row in anon_device.iterrows():
        d = row["device"]
        if d not in result["by_device"]:
            result["by_device"][d] = {"anon": 0, "known": 0, "anon_imp": 0, "known_imp": 0}
        if row["is_anonymized_query"]:
            result["by_device"][d]["anon"] = int(row["count"])
            result["by_device"][d]["anon_imp"] = int(row["impressions"])
        else:
            result["by_device"][d]["known"] = int(row["count"])
            result["by_device"][d]["known_imp"] = int(row["impressions"])

    anon_st = df.groupby(["search_type", "is_anonymized_query"]).agg(
        count=("impressions", "count"),
        impressions=("impressions", "sum"),
    ).reset_index()
    for _, row in anon_st.iterrows():
        st = row["search_type"]
        if st not in result["by_search_type"]:
            result["by_search_type"][st] = {"anon": 0, "known": 0, "anon_imp": 0, "known_imp": 0}
        if row["is_anonymized_query"]:
            result["by_search_type"][st]["anon"] = int(row["count"])
            result["by_search_type"][st]["anon_imp"] = int(row["impressions"])
        else:
            result["by_search_type"][st]["known"] = int(row["count"])
            result["by_search_type"][st]["known_imp"] = int(row["impressions"])

    return result


# Deep-but-bounded caps for the raw per-keyword/per-URL tables. A mature
# multi-market site's full keyword universe can run into the hundreds of
# thousands of rows — embedding all of it would blow the generated HTML past
# GitHub's 100MB per-file push limit. These are far deeper than a shallow
# "top 20/50" cutoff (enough for genuine searching/sorting/digging), while
# the Keyword Themes tab is what gives truly exhaustive, uncapped coverage —
# it aggregates every keyword into a bounded set of themes instead of
# listing them all individually.
TABLE_MAX_ROWS_PER_MARKET = 2000
TABLE_MAX_ROWS_ALL_MARKETS = 5000
BRAND_TOP_N = 500


def generate_url_performance(df):
    url_agg = df.groupby(["market", "url", "url_path"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        query_count=("query", lambda x: x.dropna().nunique()),
    ).reset_index()

    url_agg["avg_position"] = (url_agg["sum_position"] / url_agg["impressions"]).round(1)
    url_agg["ctr"] = (url_agg["clicks"] / url_agg["impressions"] * 100).round(2)

    result = {}
    for market in url_agg["market"].unique():
        mdf = url_agg[url_agg["market"] == market].sort_values("impressions", ascending=False).head(TABLE_MAX_ROWS_PER_MARKET)
        result[market] = [{
            "url": row["url"],
            "path": row["url_path"],
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
            "query_count": int(row["query_count"]),
        } for _, row in mdf.iterrows()]

    url_all = df.groupby(["url", "url_path", "market"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()
    url_all_agg = url_all.groupby(["url", "url_path"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        markets=("market", lambda x: list(x.unique())),
    ).reset_index()
    url_all_agg["avg_position"] = (url_all_agg["sum_position"] / url_all_agg["impressions"]).round(1)
    url_all_agg["ctr"] = (url_all_agg["clicks"] / url_all_agg["impressions"] * 100).round(2)
    top_all = url_all_agg.sort_values("impressions", ascending=False).head(TABLE_MAX_ROWS_ALL_MARKETS)
    result["All Markets"] = [{
        "url": row["url"],
        "path": row["url_path"],
        "impressions": int(row["impressions"]),
        "clicks": int(row["clicks"]),
        "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
        "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        "markets": row["markets"],
    } for _, row in top_all.iterrows()]

    return result


def generate_keyword_performance(df):
    kw_df = df[(~df["is_anonymized_query"]) & (df["query"].notna()) & (df["query"] != "")]

    kw_agg = kw_df.groupby(["market", "query"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        url_count=("url", "nunique"),
    ).reset_index()

    kw_agg["avg_position"] = (kw_agg["sum_position"] / kw_agg["impressions"]).round(1)
    kw_agg["ctr"] = (kw_agg["clicks"] / kw_agg["impressions"] * 100).round(2)

    result = {}
    for market in kw_agg["market"].unique():
        mdf = kw_agg[kw_agg["market"] == market].sort_values("impressions", ascending=False).head(TABLE_MAX_ROWS_PER_MARKET)
        result[market] = [{
            "query": row["query"],
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
            "url_count": int(row["url_count"]),
        } for _, row in mdf.iterrows()]

    kw_all = kw_df.groupby("query").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        url_count=("url", "nunique"),
        market_count=("market", "nunique"),
    ).reset_index()
    kw_all["avg_position"] = (kw_all["sum_position"] / kw_all["impressions"]).round(1)
    kw_all["ctr"] = (kw_all["clicks"] / kw_all["impressions"] * 100).round(2)
    top_all = kw_all.sort_values("impressions", ascending=False).head(TABLE_MAX_ROWS_ALL_MARKETS)
    result["All Markets"] = [{
        "query": row["query"],
        "impressions": int(row["impressions"]),
        "clicks": int(row["clicks"]),
        "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
        "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        "url_count": int(row["url_count"]),
        "market_count": int(row["market_count"]),
    } for _, row in top_all.iterrows()]

    return result


def generate_country_data(df):
    country_agg = df.groupby(["visitor_country", "country"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        anon_queries=("is_anonymized_query", "sum"),
        total_rows=("impressions", "count"),
    ).reset_index()

    country_summary = country_agg.groupby("visitor_country").agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
        anon_queries=("anon_queries", "sum"),
        total_rows=("total_rows", "sum"),
        iso=("country", "first"),
    ).reset_index()
    country_summary["avg_position"] = (country_summary["sum_position"] / country_summary["impressions"]).round(1)
    country_summary["ctr"] = (country_summary["clicks"] / country_summary["impressions"] * 100).round(2)
    country_summary["anon_pct"] = (country_summary["anon_queries"] / country_summary["total_rows"] * 100).round(1)
    country_summary = country_summary.sort_values("impressions", ascending=False)

    result = [{
        "country": row["visitor_country"],
        "iso": row["iso"],
        "impressions": int(row["impressions"]),
        "clicks": int(row["clicks"]),
        "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
        "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        "anon_pct": float(row["anon_pct"]) if pd.notna(row["anon_pct"]) else 0,
        "total_rows": int(row["total_rows"]),
    } for _, row in country_summary.iterrows()]

    country_by_market = df.groupby(["visitor_country", "market"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()

    market_breakdown = {}
    for _, row in country_by_market.iterrows():
        vc = row["visitor_country"]
        if vc not in market_breakdown:
            market_breakdown[vc] = {}
        market_breakdown[vc][row["market"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
        }

    top_countries = [r["country"] for r in result[:30]]
    daily_country = df[df["visitor_country"].isin(top_countries)].groupby(["data_date", "visitor_country"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        anon_queries=("is_anonymized_query", "sum"),
        total_rows=("impressions", "count"),
    ).reset_index()

    daily_by_country = {}
    for _, row in daily_country.iterrows():
        date = row["data_date"]
        vc = row["visitor_country"]
        if date not in daily_by_country:
            daily_by_country[date] = {}
        daily_by_country[date][vc] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "anon_queries": int(row["anon_queries"]),
            "total_rows": int(row["total_rows"]),
        }

    return {
        "summary": result,
        "by_market": market_breakdown,
        "daily": daily_by_country,
    }


def generate_device_search_data(df):
    dev_market = df.groupby(["market", "device"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()
    dev_market["avg_position"] = (dev_market["sum_position"] / dev_market["impressions"]).round(1)
    dev_market["ctr"] = (dev_market["clicks"] / dev_market["impressions"] * 100).round(2)

    by_market_device = {}
    for _, row in dev_market.iterrows():
        m = row["market"]
        if m not in by_market_device:
            by_market_device[m] = {}
        by_market_device[m][row["device"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        }

    st_market = df.groupby(["market", "search_type"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()
    st_market["avg_position"] = (st_market["sum_position"] / st_market["impressions"]).round(1)
    st_market["ctr"] = (st_market["clicks"] / st_market["impressions"] * 100).round(2)

    by_market_search = {}
    for _, row in st_market.iterrows():
        m = row["market"]
        if m not in by_market_search:
            by_market_search[m] = {}
        by_market_search[m][row["search_type"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        }

    daily_dev = df.groupby(["data_date", "device"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()

    daily_device = {}
    for _, row in daily_dev.iterrows():
        d = row["data_date"]
        if d not in daily_device:
            daily_device[d] = {}
        daily_device[d][row["device"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
        }

    daily_st = df.groupby(["data_date", "search_type"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()

    daily_search = {}
    for _, row in daily_st.iterrows():
        d = row["data_date"]
        if d not in daily_search:
            daily_search[d] = {}
        daily_search[d][row["search_type"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
        }

    return {
        "by_market_device": by_market_device,
        "by_market_search": by_market_search,
        "daily_device": daily_device,
        "daily_search": daily_search,
    }


def generate_search_features(df):
    feature_cols = [c for c in df.columns if c.startswith("is_") and c not in ["is_anonymized_query", "is_anonymized_discover"]]

    feature_summary = {}
    for col in feature_cols:
        mask = df[col].astype(str).str.lower() == "true"
        count = int(mask.sum())
        if count > 0:
            feature_name = col.replace("is_", "").replace("_", " ").title()
            imp_sum = int(df.loc[mask, "impressions"].sum())
            click_sum = int(df.loc[mask, "clicks"].sum())
            feature_summary[feature_name] = {
                "rows": count,
                "impressions": imp_sum,
                "clicks": click_sum,
                "ctr": round(click_sum / imp_sum * 100, 2) if imp_sum > 0 else 0,
            }

    feature_by_market = {}
    for market in df["market"].unique():
        mdf = df[df["market"] == market]
        market_features = {}
        for col in feature_cols:
            mask = mdf[col].astype(str).str.lower() == "true"
            count = int(mask.sum())
            if count > 0:
                feature_name = col.replace("is_", "").replace("_", " ").title()
                market_features[feature_name] = {
                    "rows": count,
                    "impressions": int(mdf.loc[mask, "impressions"].sum()),
                    "clicks": int(mdf.loc[mask, "clicks"].sum()),
                }
        if market_features:
            feature_by_market[market] = market_features

    return {
        "summary": feature_summary,
        "by_market": feature_by_market,
    }


def generate_url_daily(df):
    # Full per-date series for every URL would multiply row count by the number
    # of dates, so this stays capped (it only feeds the trend-selector dropdown —
    # the uncapped aggregate picture lives in url_performance / keyword_themes).
    top_urls = df.groupby("url")["impressions"].sum().nlargest(500).index.tolist()

    url_daily = df[df["url"].isin(top_urls)].groupby(["data_date", "url", "market"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()

    url_daily["avg_position"] = (url_daily["sum_position"] / url_daily["impressions"]).round(1)
    url_daily["ctr"] = (url_daily["clicks"] / url_daily["impressions"] * 100).round(2)

    result = {}
    for _, row in url_daily.iterrows():
        url = row["url"]
        if url not in result:
            result[url] = {"market": row["market"], "daily": {}}
        result[url]["daily"][row["data_date"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
        }

    return result


def generate_keyword_daily(df):
    kw_df = df[(~df["is_anonymized_query"]) & (df["query"].notna()) & (df["query"] != "")]
    # Same rationale as generate_url_daily: this only feeds the trend-selector
    # dropdown, so it stays capped for payload size, not for analytical depth.
    top_kws = kw_df.groupby("query")["impressions"].sum().nlargest(500).index.tolist()

    kw_daily = kw_df[kw_df["query"].isin(top_kws)].groupby(["data_date", "query"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()

    kw_daily["avg_position"] = (kw_daily["sum_position"] / kw_daily["impressions"]).round(1)

    result = {}
    for _, row in kw_daily.iterrows():
        q = row["query"]
        if q not in result:
            result[q] = {}
        result[q][row["data_date"]] = {
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
        }

    return result


# ─── New Analysis Functions ───

def load_historical_monthly():
    """Load historical_data.json (from extract_historical.py / BigQuery) for long-term trend and YoY."""
    path = DATA_DIR / "historical_data.json"
    if not path.exists():
        print("  historical_data.json not found — long-term trend and YoY unavailable")
        return None
    with open(path) as f:
        data = json.load(f)
    dates = list(data.get("daily_all_markets", {}).keys())
    if dates:
        print(f"  historical_data.json: {min(dates)} to {max(dates)}")
    return data


BRAND_RE = re.compile(r"woodupp|wood\s*-?\s*up", re.IGNORECASE)


def generate_brand_analysis(df):
    """Generate brand vs non-brand analysis from the GSC URL impressions data."""
    kw_df = df[df["query"].notna() & (df["query"] != "")].copy()
    kw_df["is_brand"] = kw_df["query"].apply(lambda q: bool(BRAND_RE.search(q)))
    anon_df = df[df["is_anonymized_query"] | df["query"].isna() | (df["query"] == "")]

    brand = kw_df[kw_df["is_brand"]]
    nonbrand = kw_df[~kw_df["is_brand"]]

    def agg_stats(sub):
        imp = int(sub["impressions"].sum())
        clk = int(sub["clicks"].sum())
        return {
            "impressions": imp,
            "clicks": clk,
            "queries": int(sub["query"].nunique()) if "query" in sub.columns else 0,
            "avg_position": round(float(sub["sum_position"].sum() / imp), 1) if imp > 0 else 0,
            "ctr": round(clk / imp * 100, 2) if imp > 0 else 0,
        }

    overall = {
        "brand": agg_stats(brand),
        "nonbrand": agg_stats(nonbrand),
        "anonymized": {
            "impressions": int(anon_df["impressions"].sum()),
            "clicks": int(anon_df["clicks"].sum()),
        },
    }

    by_market = {}
    for market in sorted(kw_df["market"].unique()):
        m_brand = brand[brand["market"] == market]
        m_nonbrand = nonbrand[nonbrand["market"] == market]
        m_anon = anon_df[anon_df["market"] == market]
        by_market[market] = {
            "brand": agg_stats(m_brand),
            "nonbrand": agg_stats(m_nonbrand),
            "anonymized": {
                "impressions": int(m_anon["impressions"].sum()),
                "clicks": int(m_anon["clicks"].sum()),
            },
        }

    daily = {}
    for (date, market), grp in kw_df.groupby(["data_date", "market"]):
        b = grp[grp["is_brand"]]
        nb = grp[~grp["is_brand"]]
        daily.setdefault(date, {})[market] = {
            "brand": {"impressions": int(b["impressions"].sum()), "clicks": int(b["clicks"].sum())},
            "nonbrand": {"impressions": int(nb["impressions"].sum()), "clicks": int(nb["clicks"].sum())},
        }

    brand_agg = brand.groupby("query").agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"), markets=("market", "nunique"),
    ).reset_index()
    brand_agg["avg_position"] = (brand_agg["sum_position"] / brand_agg["impressions"]).round(1)
    brand_agg["ctr"] = (brand_agg["clicks"] / brand_agg["impressions"] * 100).round(2)
    # to_dict('records') (not .apply(axis=1).tolist()) — apply on a zero-row
    # DataFrame returns an empty DataFrame instead of a Series, which breaks
    # .tolist(); a market/period with no brand (or no non-brand) queries at
    # all would otherwise crash this.
    top_brand = [{"query": r["query"], "impressions": int(r["impressions"]),
                  "clicks": int(r["clicks"]), "avg_position": float(r["avg_position"]),
                  "ctr": float(r["ctr"]), "markets": int(r["markets"])}
                 for r in brand_agg.sort_values("impressions", ascending=False).head(BRAND_TOP_N).to_dict("records")]

    nb_agg = nonbrand.groupby("query").agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"), markets=("market", "nunique"),
    ).reset_index()
    nb_agg["avg_position"] = (nb_agg["sum_position"] / nb_agg["impressions"]).round(1)
    nb_agg["ctr"] = (nb_agg["clicks"] / nb_agg["impressions"] * 100).round(2)
    top_nonbrand = [{"query": r["query"], "impressions": int(r["impressions"]),
                     "clicks": int(r["clicks"]), "avg_position": float(r["avg_position"]),
                     "ctr": float(r["ctr"]), "markets": int(r["markets"])}
                    for r in nb_agg.sort_values("impressions", ascending=False).head(BRAND_TOP_N).to_dict("records")]

    result = {
        "overall": overall,
        "by_market": by_market,
        "daily": daily,
        "top_brand": top_brand,
        "top_nonbrand": top_nonbrand,
        "brand_pattern": BRAND_RE.pattern + " (case-insensitive)",
    }
    print(f"  Brand analysis: {len(daily)} daily dates, {len(by_market)} markets")
    return result


def generate_monthly_trend(df, historical_monthly=None):
    """Combined monthly trend: historical BigQuery data + CSV data."""
    df = df.copy()

    all_markets = {}
    by_market = {}

    if historical_monthly:
        for month, vals in historical_monthly.get("monthly_all_markets", {}).items():
            all_markets[month] = {"impressions": vals["impressions"], "clicks": vals["clicks"], "source": "historical"}
        for mkt, months in historical_monthly.get("monthly_by_market", {}).items():
            by_market.setdefault(mkt, {})
            for month, vals in months.items():
                by_market[mkt][month] = {"impressions": vals["impressions"], "clicks": vals["clicks"], "source": "historical"}

    if df.empty or "data_date" not in df.columns:
        return {"months": sorted(all_markets.keys()), "all_markets": all_markets, "by_market": by_market}

    df["month"] = df["data_date"].str[:7]
    all_csv = df.groupby("month").agg(impressions=("impressions","sum"), clicks=("clicks","sum")).reset_index()
    by_mkt_csv = df.groupby(["month","market"]).agg(impressions=("impressions","sum"), clicks=("clicks","sum")).reset_index()

    for _, row in all_csv.iterrows():
        all_markets[row["month"]] = {"impressions": int(row["impressions"]), "clicks": int(row["clicks"]), "source": "csv"}
    for _, row in by_mkt_csv.iterrows():
        by_market.setdefault(row["market"], {})[row["month"]] = {
            "impressions": int(row["impressions"]), "clicks": int(row["clicks"]), "source": "csv"
        }

    return {"months": sorted(all_markets.keys()), "all_markets": all_markets, "by_market": by_market}


def generate_movers(df):
    """Identify keyword and URL winners/losers between two equal recent periods."""
    dates = sorted(df["data_date"].unique())
    n = len(dates)
    if n < 14:
        return {"insufficient_data": True}

    split = min(28, n // 2)
    recent_dates = set(dates[-split:])
    prior_dates = set(dates[-split * 2:-split])
    if not prior_dates:
        return {"insufficient_data": True}

    period_recent = f"{min(recent_dates)} to {max(recent_dates)}"
    period_prior = f"{min(prior_dates)} to {max(prior_dates)}"

    kw_df = df[(~df["is_anonymized_query"]) & df["query"].notna() & (df["query"] != "")]

    def agg_kw(date_set):
        sub = kw_df[kw_df["data_date"].isin(date_set)]
        agg = sub.groupby(["query", "market"]).agg(
            impressions=("impressions", "sum"), clicks=("clicks", "sum"), sum_position=("sum_position", "sum"),
        ).reset_index()
        agg["avg_position"] = (agg["sum_position"] / agg["impressions"]).round(1)
        return agg

    kw_r = agg_kw(recent_dates)
    kw_p = agg_kw(prior_dates)
    kw_m = kw_r.merge(kw_p, on=["query", "market"], how="outer", suffixes=("_r", "_p")).fillna(0)
    kw_m["imp_change"] = (kw_m["impressions_r"] - kw_m["impressions_p"]).astype(int)
    kw_m["pos_change"] = (kw_m["avg_position_r"] - kw_m["avg_position_p"]).round(1)
    kw_sig = kw_m[kw_m["impressions_p"] >= 50]

    def agg_url(date_set):
        sub = df[df["data_date"].isin(date_set)]
        agg = sub.groupby(["url", "url_path", "market"]).agg(
            impressions=("impressions", "sum"), clicks=("clicks", "sum"), sum_position=("sum_position", "sum"),
        ).reset_index()
        agg["avg_position"] = (agg["sum_position"] / agg["impressions"]).round(1)
        return agg

    url_r = agg_url(recent_dates)
    url_p = agg_url(prior_dates)
    url_m = url_r.merge(url_p, on=["url", "url_path", "market"], how="outer", suffixes=("_r", "_p")).fillna(0)
    url_m["imp_change"] = (url_m["impressions_r"] - url_m["impressions_p"]).astype(int)
    url_sig = url_m[url_m["impressions_p"] >= 100]

    def kw_rec(row):
        return {
            "query": row["query"], "market": row["market"],
            "imp_recent": int(row["impressions_r"]), "imp_prior": int(row["impressions_p"]),
            "clicks_recent": int(row["clicks_r"]), "clicks_prior": int(row["clicks_p"]),
            "imp_change": int(row["imp_change"]),
            "imp_pct": round((row["impressions_r"] - row["impressions_p"]) / row["impressions_p"] * 100, 1) if row["impressions_p"] > 0 else 0,
            "pos_recent": float(row["avg_position_r"]), "pos_prior": float(row["avg_position_p"]),
            "pos_change": float(row["pos_change"]),
        }

    def url_rec(row):
        return {
            "url": row["url"], "path": row["url_path"], "market": row["market"],
            "imp_recent": int(row["impressions_r"]), "imp_prior": int(row["impressions_p"]),
            "clicks_recent": int(row["clicks_r"]), "clicks_prior": int(row["clicks_p"]),
            "imp_change": int(row["imp_change"]),
            "imp_pct": round((row["impressions_r"] - row["impressions_p"]) / row["impressions_p"] * 100, 1) if row["impressions_p"] > 0 else 0,
        }

    pos_sig = kw_sig[(kw_sig["avg_position_p"] > 0) & (kw_sig["avg_position_r"] > 0)]

    return {
        "period_recent": period_recent,
        "period_prior": period_prior,
        "split_days": split,
        "keyword_winners": [kw_rec(r) for _, r in kw_sig.nlargest(300, "imp_change").iterrows()],
        "keyword_losers": [kw_rec(r) for _, r in kw_sig.nsmallest(300, "imp_change").iterrows()],
        "url_winners": [url_rec(r) for _, r in url_sig.nlargest(300, "imp_change").iterrows()],
        "url_losers": [url_rec(r) for _, r in url_sig.nsmallest(300, "imp_change").iterrows()],
        "pos_gainers": [kw_rec(r) for _, r in pos_sig.nsmallest(300, "pos_change").iterrows()],
        "pos_losers": [kw_rec(r) for _, r in pos_sig.nlargest(300, "pos_change").iterrows()],
    }


# ─── Keyword Theme / Topic Analysis ───
#
# Groups the full (uncapped) keyword set into recurring word/phrase "themes"
# (e.g. "oak", "dining table", "garden bench") using stopword-filtered
# unigrams + adjacent-token bigrams — a lightweight n-gram co-occurrence
# technique that needs no extra ML dependency, so it runs anywhere pandas
# does. It answers "is this group of keywords trending up, and in which
# markets" at a level above individual queries, without truncating which
# keywords are allowed to contribute.

# Purely linguistic function-word lists (no business-word removal) covering
# WoodUpp's markets, so theme extraction isn't dominated by "the/und/de/og" etc.
STOPWORDS_EN = {
    "a","an","the","and","or","but","if","of","at","by","for","with","about","against",
    "between","into","through","during","before","after","above","below","to","from","up",
    "down","in","out","on","off","over","under","again","further","then","once","here","there",
    "when","where","why","how","all","any","both","each","few","more","most","other","some",
    "such","no","nor","not","only","own","same","so","than","too","very","s","t","can","will",
    "just","don","should","now","is","are","was","were","be","been","being","have","has","had",
    "having","do","does","did","doing","i","me","my","we","our","you","your","he","him","his",
    "she","her","it","its","they","them","their","what","which","who","whom","this","that",
    "these","those","am","vs","new","best",
}
STOPWORDS_DE = {
    "der","die","das","und","oder","aber","wenn","von","bei","für","mit","gegen","zwischen",
    "in","durch","während","vor","nach","über","unter","zu","aus","auf","ab","wieder","dann",
    "hier","da","wann","wo","warum","wie","alle","jede","jeder","jedes","beide","wenige","mehr",
    "andere","einige","solche","nicht","nur","gleiche","so","als","ist","sind","war","waren",
    "sein","gewesen","haben","hat","hatte","ich","mich","mein","wir","unser","du","dein","er",
    "ihm","sie","ihr","ihre","es","ihnen","was","welche","wer","dies","ein","eine","einen",
    "einem","einer","im","am","zum","zur","den","dem","des","kein","keine","für","neu",
}
STOPWORDS_FR = {
    "le","la","les","un","une","des","et","ou","mais","si","de","à","pour","avec","contre",
    "entre","dans","par","pendant","avant","après","sur","sous","encore","puis","ici","là",
    "quand","où","pourquoi","comment","tout","toute","tous","toutes","chaque","plus","autre",
    "quelque","tel","ne","pas","seulement","même","aussi","très","être","est","sont","était",
    "étaient","avoir","ai","as","a","ont","je","me","mon","nous","notre","tu","ton","il","lui",
    "son","elle","sa","ils","elles","leur","ce","cette","ces","qui","que","quoi","du","au","aux",
    "nouveau","meilleur",
}
STOPWORDS_ES = {
    "el","la","los","las","un","una","unos","unas","y","o","pero","si","de","a","por","para",
    "con","contra","entre","en","durante","antes","después","sobre","bajo","otra","vez","luego",
    "aquí","allí","cuando","donde","como","todo","toda","todos","todas","cada","más","otro",
    "algún","alguna","tal","no","solo","mismo","tan","ser","es","son","era","eran","tener",
    "tengo","tiene","yo","mi","nosotros","nuestro","tú","tu","él","su","ella","ellos","ellas",
    "este","esta","estos","estas","que","del","al","por qué","nuevo","mejor",
}
STOPWORDS_IT = {
    "il","lo","la","i","gli","le","un","uno","una","e","o","ma","se","di","a","per","con",
    "contro","tra","fra","in","durante","prima","dopo","su","sotto","ancora","poi","qui","qua",
    "là","quando","dove","perché","come","tutto","tutta","tutti","tutte","ogni","più","altro",
    "alcuni","alcune","tale","non","solo","stesso","così","essere","è","sono","era","erano",
    "avere","ho","hai","ha","abbiamo","io","mio","noi","nostro","tu","tuo","lui","suo","lei",
    "loro","questo","questa","questi","queste","che","del","della","dei","delle","nuovo","migliore",
}
STOPWORDS_NL = {
    "de","het","een","en","of","maar","als","van","bij","voor","met","tegen","tussen","in",
    "door","tijdens","na","boven","onder","naar","uit","op","af","weer","dan","hier","daar",
    "wanneer","waar","waarom","hoe","alle","elke","beide","weinig","meer","andere","enkele",
    "zo","niet","alleen","zelfde","zeer","zijn","is","was","hebben","heb","heeft","had","ik",
    "mijn","wij","ons","jij","jouw","hij","hem","zij","haar","hun","dit","dat","deze","die",
    "wat","welke","wie","nieuw","beste",
}
STOPWORDS_DA = {
    "den","det","en","et","og","eller","men","hvis","af","ved","for","med","mod","mellem","i",
    "gennem","under","før","efter","over","til","fra","op","ned","ud","på","igen","så","her",
    "der","hvornår","hvor","hvorfor","hvordan","alle","hver","begge","mere","andre","nogle",
    "sådan","ikke","kun","samme","meget","være","er","var","have","har","havde","jeg","mig",
    "min","vi","vores","du","din","han","ham","hans","hun","hende","hendes","de","dem","deres",
    "denne","dette","disse","hvad","hvilken","hvem","ny","bedste",
}
STOPWORDS_NO = {
    "den","det","en","et","og","eller","men","hvis","av","ved","for","med","mot","mellom","i",
    "gjennom","under","før","etter","over","til","fra","opp","ned","ut","på","igjen","så","her",
    "der","når","hvor","hvorfor","hvordan","alle","hver","begge","mer","andre","noen","slik",
    "ikke","bare","samme","veldig","være","er","var","ha","har","hadde","jeg","meg","min","vi",
    "vår","du","din","han","ham","hans","hun","henne","hennes","de","dem","deres","denne",
    "dette","disse","hva","hvilken","hvem","ny","beste",
}
STOPWORDS_SV = {
    "den","det","en","ett","och","eller","men","om","av","vid","för","med","mot","mellan","i",
    "genom","under","före","efter","över","till","från","upp","ner","ut","på","igen","så","här",
    "där","när","var","varför","hur","alla","varje","båda","mer","andra","några","sådan","inte",
    "bara","samma","mycket","vara","är","ha","har","hade","jag","mig","min","vi","vår","du",
    "din","han","honom","hans","hon","henne","hennes","de","dem","deras","denna","detta",
    "dessa","vad","vilken","vem","ny","bästa",
}
STOPWORDS_PT = {
    "o","a","os","as","um","uma","uns","umas","e","ou","mas","se","de","por","para","com",
    "contra","entre","em","durante","antes","depois","sobre","sob","outra","vez","então","aqui",
    "ali","quando","onde","porque","como","todo","toda","todos","todas","cada","mais","outro",
    "algum","alguma","tal","não","apenas","mesmo","tão","ser","é","são","era","eram","ter",
    "tenho","tem","eu","meu","nós","nosso","tu","teu","ele","seu","ela","sua","eles","elas",
    "este","esta","estes","estas","que","do","da","dos","das","no","na","novo","melhor",
}
STOPWORDS_PL = {
    "i","oraz","lub","ale","jeśli","z","przy","dla","ze","przeciwko","między","w","przez",
    "podczas","przed","po","nad","pod","do","od","znowu","wtedy","tutaj","tam","kiedy","gdzie",
    "dlaczego","jak","wszystko","każdy","oba","więcej","inne","kilka","taki","nie","tylko","tak",
    "bardzo","być","jest","są","był","była","było","mieć","mam","masz","ma","ja","mój","my",
    "nasz","ty","twój","on","jego","ona","jej","oni","one","ich","ten","ta","to","te","co",
    "który","kto","nowy","najlepszy",
}

STOPWORDS = (
    STOPWORDS_EN | STOPWORDS_DE | STOPWORDS_FR | STOPWORDS_ES | STOPWORDS_IT | STOPWORDS_NL |
    STOPWORDS_DA | STOPWORDS_NO | STOPWORDS_SV | STOPWORDS_PT | STOPWORDS_PL
)
BRAND_TOKENS = {"woodupp", "woodup"}
THEME_TOKEN_RE = re.compile(r"[^\W\d_]+", re.UNICODE)

THEME_MIN_QUERIES = 3        # a theme must be shared by at least this many distinct keywords
THEME_MIN_IMPRESSIONS = 100  # ...and account for at least this many impressions to matter
THEME_MAX_COUNT = 3000       # hard ceiling on how many themes are kept, by total impressions
THEME_MAX_SAMPLE_KEYWORDS = 50  # per-theme keyword drill-down list — a payload-size guard,
                                 # not an analysis cutoff (theme totals always use every keyword)


def tokenize_query(query):
    if not isinstance(query, str) or not query:
        return []
    toks = THEME_TOKEN_RE.findall(query.lower())
    return [t for t in toks if len(t) >= 2 and t not in STOPWORDS and t not in BRAND_TOKENS]


def theme_candidates(tokens):
    """Unigrams + adjacent-token bigrams, e.g. ['oak','dining','table'] ->
    {'oak','dining','table','oak dining','dining table'}."""
    themes = set(tokens)
    for i in range(len(tokens) - 1):
        themes.add(f"{tokens[i]} {tokens[i+1]}")
    return themes


def generate_keyword_themes(df):
    """Theme-level rollup of every non-anonymized keyword: overall performance,
    per-market breakdown, weekly trend, and recent-vs-prior movers per
    theme/market — the deep "is this topic growing, and where" view."""
    kw_df = df[(~df["is_anonymized_query"]) & df["query"].notna() & (df["query"] != "")].copy()
    if kw_df.empty:
        return {"insufficient_data": True}

    unique_queries = kw_df["query"].unique()
    query_themes = {q: theme_candidates(tokenize_query(q)) for q in unique_queries}

    query_totals = kw_df.groupby("query")["impressions"].sum()
    theme_support = defaultdict(int)
    theme_impressions = defaultdict(int)
    for q, themes in query_themes.items():
        imp = int(query_totals.get(q, 0))
        for th in themes:
            theme_support[th] += 1
            theme_impressions[th] += imp

    significant = {
        th for th, cnt in theme_support.items()
        if cnt >= THEME_MIN_QUERIES and theme_impressions[th] >= THEME_MIN_IMPRESSIONS
    }
    if not significant:
        return {"insufficient_data": True}

    # Hard ceiling on how many themes carry through, keyed by total impressions.
    # 20 markets across ~11 languages can surface a much bigger recurring
    # vocabulary than a single-language test does, and every downstream
    # structure (weekly trend, per-market breakdown, keyword samples) scales
    # with theme count — this keeps the payload predictable regardless of how
    # diverse the real keyword set turns out to be.
    if len(significant) > THEME_MAX_COUNT:
        significant = set(sorted(significant, key=lambda th: theme_impressions[th], reverse=True)[:THEME_MAX_COUNT])

    q2themes = {q: [t for t in themes if t in significant] for q, themes in query_themes.items()}
    q2themes = {q: t for q, t in q2themes.items() if t}

    # A query can map to several themes, and a full pandas .explode() over
    # every (query, market[, week]) row would multiply row count by
    # themes-per-query — at millions of keywords that's tens of millions of
    # rows materialized at once. Instead, stream each pre-aggregated table
    # through once and fold straight into small per-theme accumulator dicts
    # (bounded by the number of significant themes, not by raw row count).
    def stream_theme_group(agg_df, group_col):
        """Single pass over a (query, <group_col>, impressions, clicks,
        sum_position) table. Returns dict[(theme, group_value)] ->
        [impressions, clicks, sum_position, row_count]."""
        stats = defaultdict(lambda: [0, 0, 0.0, 0])
        for row in agg_df.itertuples(index=False):
            themes = q2themes.get(row.query)
            if not themes:
                continue
            impressions, clicks, sum_position = row.impressions, row.clicks, row.sum_position
            group_value = getattr(row, group_col)
            for th in themes:
                s = stats[(th, group_value)]
                s[0] += impressions
                s[1] += clicks
                s[2] += sum_position
                s[3] += 1
        return stats

    def rollup_by_theme(stats):
        """Collapse a dict[(theme, group)] -> [...] into dict[theme] -> [impressions, clicks, sum_position, {groups}]."""
        rolled = defaultdict(lambda: [0, 0, 0.0, set()])
        for (theme, group_value), (imp, clk, sp, _cnt) in stats.items():
            r = rolled[theme]
            r[0] += imp; r[1] += clk; r[2] += sp; r[3].add(group_value)
        return rolled

    def avg_pos(sum_position, impressions):
        return round(sum_position / impressions, 1) if impressions > 0 else 0

    def ctr_of(clicks, impressions):
        return round(clicks / impressions * 100, 2) if impressions > 0 else 0

    # ── Theme x market totals (all dates) ──
    qm_agg = kw_df.groupby(["query", "market"]).agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()
    theme_market_stats = stream_theme_group(qm_agg, "market")

    theme_market_out = [{
        "theme": theme, "market": market,
        "impressions": int(imp), "clicks": int(clk),
        "ctr": ctr_of(clk, imp), "avg_position": avg_pos(sp, imp),
        "keyword_count": int(kc),
    } for (theme, market), (imp, clk, sp, kc) in theme_market_stats.items()]
    theme_market_out.sort(key=lambda r: r["impressions"], reverse=True)

    theme_rolled = rollup_by_theme(theme_market_stats)
    themes_out = [{
        "theme": theme,
        "type": "phrase" if " " in theme else "word",
        "impressions": int(imp), "clicks": int(clk),
        "ctr": ctr_of(clk, imp), "avg_position": avg_pos(sp, imp),
        "keyword_count": theme_support[theme],  # distinct keywords globally, already counted above
        "market_count": len(markets),
    } for theme, (imp, clk, sp, markets) in theme_rolled.items()]
    themes_out.sort(key=lambda r: r["impressions"], reverse=True)

    # ── Weekly trend per theme (across all markets) ──
    kw_df["week"] = pd.to_datetime(kw_df["data_date"]).dt.to_period("W-SUN").apply(
        lambda p: p.start_time.strftime("%Y-%m-%d"))
    qw_agg = kw_df.groupby(["query", "week"]).agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"),
    ).reset_index()
    theme_week_stats = stream_theme_group(qw_agg, "week")

    trend_weekly = {}
    for (theme, week), (imp, clk, sp, _cnt) in theme_week_stats.items():
        trend_weekly.setdefault(theme, {})[week] = {
            "impressions": int(imp), "clicks": int(clk), "avg_position": avg_pos(sp, imp),
        }

    # ── Recent-vs-prior movers, per theme+market and per theme overall ──
    dates = sorted(kw_df["data_date"].unique())
    n = len(dates)
    movers = {"insufficient_data": True}
    if n >= 14:
        split = min(28, n // 2)
        recent_dates = set(dates[-split:])
        prior_dates = set(dates[-split * 2:-split])
        if prior_dates:
            def period_theme_market_stats(date_set):
                sub = kw_df[kw_df["data_date"].isin(date_set)]
                agg = sub.groupby(["query", "market"]).agg(
                    impressions=("impressions", "sum"), clicks=("clicks", "sum"),
                    sum_position=("sum_position", "sum"),
                ).reset_index()
                return stream_theme_group(agg, "market")

            recent_stats = period_theme_market_stats(recent_dates)
            prior_stats = period_theme_market_stats(prior_dates)

            def mover_rec(theme, market, r, p):
                imp_r, clk_r, sp_r = r[0], r[1], r[2]
                imp_p, clk_p, sp_p = p[0], p[1], p[2]
                pos_r, pos_p = avg_pos(sp_r, imp_r), avg_pos(sp_p, imp_p)
                return {
                    "theme": theme, "market": market,
                    "imp_recent": int(imp_r), "imp_prior": int(imp_p),
                    "clicks_recent": int(clk_r), "clicks_prior": int(clk_p),
                    "imp_change": int(imp_r - imp_p),
                    "imp_pct": round((imp_r - imp_p) / imp_p * 100, 1) if imp_p > 0 else 0,
                    "pos_recent": float(pos_r), "pos_prior": float(pos_p),
                    "pos_change": round(pos_r - pos_p, 1) if pos_r > 0 and pos_p > 0 else 0,
                }

            zero = [0, 0, 0.0, 0]
            theme_market_movers = []
            for key in set(recent_stats) | set(prior_stats):
                theme, market = key
                p = prior_stats.get(key, zero)
                if p[0] < THEME_MIN_IMPRESSIONS:
                    continue
                theme_market_movers.append(mover_rec(theme, market, recent_stats.get(key, zero), p))
            theme_market_movers.sort(key=lambda r: r["imp_change"], reverse=True)

            recent_by_theme = rollup_by_theme(recent_stats)
            prior_by_theme = rollup_by_theme(prior_stats)
            theme_overall_movers = []
            for theme in set(recent_by_theme) | set(prior_by_theme):
                r = recent_by_theme.get(theme, (0, 0, 0.0, None))
                p = prior_by_theme.get(theme, (0, 0, 0.0, None))
                imp_r, clk_r = r[0], r[1]
                imp_p, clk_p = p[0], p[1]
                if imp_p < THEME_MIN_IMPRESSIONS:
                    continue
                theme_overall_movers.append({
                    "theme": theme,
                    "imp_recent": int(imp_r), "imp_prior": int(imp_p),
                    "clicks_recent": int(clk_r), "clicks_prior": int(clk_p),
                    "imp_change": int(imp_r - imp_p),
                    "imp_pct": round((imp_r - imp_p) / imp_p * 100, 1) if imp_p > 0 else 0,
                })
            theme_overall_movers.sort(key=lambda r: r["imp_change"], reverse=True)

            movers = {
                "period_recent": f"{min(recent_dates)} to {max(recent_dates)}",
                "period_prior": f"{min(prior_dates)} to {max(prior_dates)}",
                "theme_market_movers": theme_market_movers,
                "theme_overall_movers": theme_overall_movers,
            }

    # ── Sample keywords per theme for drill-down ──
    # Bounded min-heap per theme (size THEME_MAX_SAMPLE_KEYWORDS) instead of
    # collecting every matching keyword and truncating afterward — keeps peak
    # memory at significant_themes × THEME_MAX_SAMPLE_KEYWORDS regardless of
    # how many keywords roll into a broad theme like "table".
    q_totals = kw_df.groupby("query").agg(
        impressions=("impressions", "sum"), clicks=("clicks", "sum"),
        sum_position=("sum_position", "sum"), market_count=("market", "nunique"),
    ).reset_index()

    theme_keyword_heaps = defaultdict(list)
    tiebreak = itertools.count()
    for row in q_totals.itertuples(index=False):
        themes = q2themes.get(row.query)
        if not themes:
            continue
        rec = {
            "query": row.query, "impressions": int(row.impressions), "clicks": int(row.clicks),
            "ctr": ctr_of(row.clicks, row.impressions), "avg_position": avg_pos(row.sum_position, row.impressions),
            "market_count": int(row.market_count),
        }
        item = (row.impressions, next(tiebreak), rec)
        for th in themes:
            heap = theme_keyword_heaps[th]
            if len(heap) < THEME_MAX_SAMPLE_KEYWORDS:
                heapq.heappush(heap, item)
            elif item[0] > heap[0][0]:
                heapq.heapreplace(heap, item)

    keywords_by_theme = {
        theme: [rec for _imp, _tie, rec in sorted(heap, key=lambda x: x[0], reverse=True)]
        for theme, heap in theme_keyword_heaps.items()
    }

    print(f"  Keyword themes: {len(themes_out)} significant themes from {len(unique_queries):,} unique keywords")

    return {
        "generated_from_queries": int(len(unique_queries)),
        "min_queries_threshold": THEME_MIN_QUERIES,
        "min_impressions_threshold": THEME_MIN_IMPRESSIONS,
        "themes": themes_out,
        "theme_market": theme_market_out,
        "trend_weekly": trend_weekly,
        "movers": movers,
        "keywords_by_theme": keywords_by_theme,
    }


# ─── Historical Data Loading & Merging ───

def load_historical():
    """Load frozen historical JSON data from data/historical/."""
    historical = {}
    data_files = [
        "overview", "daily_metrics", "anonymized", "url_performance",
        "keyword_performance", "country_data", "device_search",
        "serp_features", "url_daily", "keyword_daily",
    ]
    for name in data_files:
        path = HISTORICAL_DIR / f"{name}.json"
        if path.exists():
            with open(path) as f:
                historical[name] = json.load(f)
            print(f"  Loaded historical {name}.json")
    return historical


def merge_date_keyed(new_data, historical_data):
    """Merge date-keyed dicts: historical fills gaps, new data takes precedence."""
    if not historical_data:
        return new_data
    merged = dict(historical_data)
    merged.update(new_data)
    return merged


def merge_nested_date_keyed(new_data, historical_data):
    """Merge nested date-keyed data (e.g., url_daily: url -> daily -> date -> metrics).
    For each top-level key, merge the date entries."""
    if not historical_data:
        return new_data
    merged = dict(historical_data)
    for key, value in new_data.items():
        if key in merged:
            if isinstance(value, dict) and "daily" in value:
                # url_daily format: {url: {market, daily: {date: metrics}}}
                merged[key]["daily"] = merge_date_keyed(
                    value.get("daily", {}),
                    merged[key].get("daily", {})
                )
                merged[key]["market"] = value.get("market", merged[key].get("market"))
            elif isinstance(value, dict) and all(isinstance(v, dict) for v in value.values()):
                # keyword_daily format: {keyword: {date: metrics}}
                merged[key] = merge_date_keyed(value, merged[key])
            else:
                merged[key] = value
        else:
            merged[key] = value
    return merged


def merge_with_historical(new_data, historical, historical_monthly=None):
    """Merge all datasets: CSV data takes precedence, historical fills date gaps.
    historical_monthly comes from historical_data.json (BigQuery extract)."""
    if not historical and not historical_monthly:
        return new_data

    merged = {}

    # Date-keyed datasets: merge by date (historical fills gaps)
    merged["daily_metrics"] = merge_date_keyed(
        new_data["daily_metrics"], historical.get("daily_metrics", {}) if historical else {})

    # Anonymized: merge by_market_date keys
    merged_anon = dict(new_data["anonymized"])
    hist_anon = historical.get("anonymized", {}) if historical else {}
    if hist_anon.get("by_market_date"):
        merged_bmd = dict(hist_anon["by_market_date"])
        merged_bmd.update(new_data["anonymized"].get("by_market_date", {}))
        merged_anon["by_market_date"] = merged_bmd
    merged["anonymized"] = merged_anon

    # Country data: merge daily
    merged_country = dict(new_data["country_data"])
    hist_country = historical.get("country_data", {}) if historical else {}
    if hist_country.get("daily"):
        merged_country["daily"] = merge_date_keyed(
            new_data["country_data"].get("daily", {}), hist_country["daily"])
    merged["country_data"] = merged_country

    # Device/search: merge daily_device and daily_search
    merged_ds = dict(new_data["device_search"])
    hist_ds = historical.get("device_search", {}) if historical else {}
    if hist_ds.get("daily_device"):
        merged_ds["daily_device"] = merge_date_keyed(
            new_data["device_search"].get("daily_device", {}), hist_ds["daily_device"])
    if hist_ds.get("daily_search"):
        merged_ds["daily_search"] = merge_date_keyed(
            new_data["device_search"].get("daily_search", {}), hist_ds["daily_search"])
    merged["device_search"] = merged_ds

    # URL daily and keyword daily: nested merge
    merged["url_daily"] = merge_nested_date_keyed(
        new_data["url_daily"], historical.get("url_daily", {}) if historical else {})
    merged["keyword_daily"] = merge_nested_date_keyed(
        new_data["keyword_daily"], historical.get("keyword_daily", {}) if historical else {})

    # Incorporate historical_monthly daily data into daily_metrics (for YoY lookups)
    if historical_monthly:
        for date, vals in historical_monthly.get("daily_all_markets", {}).items():
            if date not in merged["daily_metrics"]:
                merged["daily_metrics"][date] = {}
            if "All Markets" not in merged["daily_metrics"][date]:
                imp = vals["impressions"]
                clk = vals["clicks"]
                merged["daily_metrics"][date]["All Markets"] = {
                    "impressions": imp, "clicks": clk,
                    "avg_position": 0,
                    "ctr": round(clk / imp * 100, 2) if imp > 0 else 0,
                    "total_rows": 0, "anon_queries": 0, "anon_pct": 0,
                }
        for market_name, dates_data in historical_monthly.get("daily_by_market", {}).items():
            for date, vals in dates_data.items():
                if date not in merged["daily_metrics"]:
                    merged["daily_metrics"][date] = {}
                if market_name not in merged["daily_metrics"][date]:
                    imp = vals["impressions"]
                    clk = vals["clicks"]
                    merged["daily_metrics"][date][market_name] = {
                        "impressions": imp, "clicks": clk,
                        "avg_position": 0,
                        "ctr": round(clk / imp * 100, 2) if imp > 0 else 0,
                        "total_rows": 0, "anon_queries": 0, "anon_pct": 0,
                    }

    # Update overview dates from merged daily_metrics
    merged["overview"] = dict(new_data["overview"])
    merged["overview"]["dates"] = sorted(merged["daily_metrics"].keys())

    # Aggregate datasets use CSV-derived values (most complete)
    merged["url_performance"] = new_data["url_performance"]
    merged["keyword_performance"] = new_data["keyword_performance"]
    merged["serp_features"] = new_data["serp_features"]

    return merged


# ─── HTML Generation ───

def generate_html(all_data):
    """Read template.html and inject data to create a standalone index.html."""
    print("Reading template.html...")
    with open(TEMPLATE_HTML, "r", encoding="utf-8") as f:
        template = f.read()

    # Build the data injection block
    data_keys = [
        "overview", "daily_metrics", "anonymized", "url_performance",
        "keyword_performance", "country_data", "device_search",
        "serp_features", "url_daily", "keyword_daily",
        "movers", "monthly_trend", "brand_analysis", "keyword_themes",
    ]
    lines = []
    for key in data_keys:
        if key in all_data:
            lines.append(f"DATA['{key}'] = {json.dumps(all_data[key], ensure_ascii=False)};")
    injection = "\n".join(lines)

    # Replace the placeholder
    marker = "// __DATA_INJECTION_POINT__"
    if marker not in template:
        raise RuntimeError(f"Marker '{marker}' not found in template.html")
    html = template.replace(marker, injection)

    print(f"Writing {OUTPUT_HTML}...")
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    size_mb = os.path.getsize(OUTPUT_HTML) / (1024 * 1024)
    print(f"  Generated index.html ({size_mb:.1f} MB)")


# ─── Main ───

def main():
    # Determine CSV path
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = DEFAULT_CSV

    # Load historical data (frozen JSON baseline)
    print("Loading historical data...")
    historical = load_historical()

    # Load long-term BigQuery history (historical_data.json)
    print("Loading historical monthly data...")
    historical_monthly = load_historical_monthly()

    # Process CSV if available
    if csv_path.exists():
        df = load_and_clean(csv_path)

        print("\nGenerating datasets from CSV...")
        new_data = {
            "overview": generate_overview(df),
            "daily_metrics": generate_daily_metrics(df),
            "anonymized": generate_anonymized_data(df),
            "url_performance": generate_url_performance(df),
            "keyword_performance": generate_keyword_performance(df),
            "country_data": generate_country_data(df),
            "device_search": generate_device_search_data(df),
            "serp_features": generate_search_features(df),
            "url_daily": generate_url_daily(df),
            "keyword_daily": generate_keyword_daily(df),
        }

        # Merge with historical
        print("\nMerging with historical data...")
        all_data = merge_with_historical(new_data, historical, historical_monthly)

        # Movers & Shakers analysis
        print("Generating movers analysis...")
        all_data["movers"] = generate_movers(df)

        # Long-term monthly trend
        print("Generating monthly trend...")
        all_data["monthly_trend"] = generate_monthly_trend(df, historical_monthly)

        # Brand vs non-brand analysis (generated from CSV)
        print("Generating brand analysis...")
        all_data["brand_analysis"] = generate_brand_analysis(df)

        # Keyword theme / topic analysis
        print("Generating keyword theme analysis...")
        all_data["keyword_themes"] = generate_keyword_themes(df)

    elif historical:
        print(f"\nCSV not found at {csv_path}, using historical data only.")
        all_data = historical
        all_data["movers"] = {"insufficient_data": True}
        all_data["keyword_themes"] = {"insufficient_data": True}
        all_data["monthly_trend"] = generate_monthly_trend(
            pd.DataFrame(), historical_monthly
        ) if historical_monthly else {"months": [], "all_markets": {}, "by_market": {}}
    else:
        print(f"\nERROR: No CSV found at {csv_path} and no historical data available.")
        sys.exit(1)

    # Generate standalone HTML
    print("\nGenerating standalone HTML dashboard...")
    generate_html(all_data)

    print("\nDone! Open index.html in a browser to view the dashboard.")


if __name__ == "__main__":
    main()
