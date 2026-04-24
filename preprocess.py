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
import sys
import os
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
        mdf = url_agg[url_agg["market"] == market].sort_values("impressions", ascending=False).head(100)
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
    top_all = url_all_agg.sort_values("impressions", ascending=False).head(200)
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
        mdf = kw_agg[kw_agg["market"] == market].sort_values("impressions", ascending=False).head(150)
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
    top_all = kw_all.sort_values("impressions", ascending=False).head(200)
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
    top_urls = df.groupby("url")["impressions"].sum().nlargest(50).index.tolist()

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
    top_kws = kw_df.groupby("query")["impressions"].sum().nlargest(50).index.tolist()

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


def load_ga4_data():
    """Load GA4 analytics data (from BigQuery ga4/ga4_events tables)."""
    path = DATA_DIR / "ga4_data.json"
    if not path.exists():
        print("  ga4_data.json not found — Analytics tab unavailable")
        return None
    with open(path) as f:
        data = json.load(f)
    print(f"  ga4_data.json: {len(data.get('months',[]))} months, {len(data.get('daily',{}))} daily dates")
    return data


def generate_monthly_trend(df, historical_monthly=None):
    """Combined monthly trend: historical BigQuery data + CSV data."""
    df = df.copy()
    df["month"] = df["data_date"].str[:7]

    all_csv = df.groupby("month").agg(impressions=("impressions","sum"), clicks=("clicks","sum")).reset_index()
    by_mkt_csv = df.groupby(["month","market"]).agg(impressions=("impressions","sum"), clicks=("clicks","sum")).reset_index()

    all_markets = {}
    by_market = {}

    if historical_monthly:
        for month, vals in historical_monthly.get("monthly_all_markets", {}).items():
            all_markets[month] = {"impressions": vals["impressions"], "clicks": vals["clicks"], "source": "historical"}
        for mkt, months in historical_monthly.get("monthly_by_market", {}).items():
            by_market.setdefault(mkt, {})
            for month, vals in months.items():
                by_market[mkt][month] = {"impressions": vals["impressions"], "clicks": vals["clicks"], "source": "historical"}

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
        "keyword_winners": [kw_rec(r) for _, r in kw_sig.nlargest(50, "imp_change").iterrows()],
        "keyword_losers": [kw_rec(r) for _, r in kw_sig.nsmallest(50, "imp_change").iterrows()],
        "url_winners": [url_rec(r) for _, r in url_sig.nlargest(50, "imp_change").iterrows()],
        "url_losers": [url_rec(r) for _, r in url_sig.nsmallest(50, "imp_change").iterrows()],
        "pos_gainers": [kw_rec(r) for _, r in pos_sig.nsmallest(50, "pos_change").iterrows()],
        "pos_losers": [kw_rec(r) for _, r in pos_sig.nlargest(50, "pos_change").iterrows()],
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
        "movers", "monthly_trend", "ga4",
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

    # Load GA4 analytics data
    print("Loading GA4 analytics data...")
    ga4_data = load_ga4_data()

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

        # GA4 analytics
        if ga4_data:
            all_data["ga4"] = ga4_data

    elif historical:
        print(f"\nCSV not found at {csv_path}, using historical data only.")
        all_data = historical
        all_data["movers"] = {"insufficient_data": True}
        all_data["monthly_trend"] = generate_monthly_trend(
            pd.DataFrame(), historical_monthly
        ) if historical_monthly else {"months": [], "all_markets": {}, "by_market": {}}
        if ga4_data:
            all_data["ga4"] = ga4_data
    else:
        print(f"\nERROR: No CSV found at {csv_path} and no historical data available.")
        sys.exit(1)

    # Generate standalone HTML
    print("\nGenerating standalone HTML dashboard...")
    generate_html(all_data)

    print("\nDone! Open index.html in a browser to view the dashboard.")


if __name__ == "__main__":
    main()
