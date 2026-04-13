"""
Preprocesses the WoodUpp GSC data into optimized JSON files for the dashboard.
Handles domain detection, country mapping, and aggregation.

Also optionally pulls Google Analytics 4 data from BigQuery if credentials are
available. Looks for a service account JSON one directory above the project
folder (same place as the GSC CSV). Falls back gracefully if missing.
"""
import pandas as pd
import json
import os
import re
from datetime import date, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SOURCE_CSV = Path(__file__).parent.parent / "woodupp_url_impressions.csv"

# ─── BigQuery / GA4 configuration ───
GA4_PROJECT_ID = "obsidian-375910"
GA4_TABLE = "obsidian-375910.woodupp.ga4"
GA4_YEARS_BACK = 2  # pull last 2 years of GA4 data

def find_ga4_credentials():
    """Look for a GA4 service account JSON one level above the project dir.

    Priority:
    1. GOOGLE_APPLICATION_CREDENTIALS env var (if set)
    2. Any file matching obsidian-*.json in the parent directory
    """
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env and Path(env).exists():
        return Path(env)
    parent = Path(__file__).parent.parent
    candidates = sorted(parent.glob("obsidian-*.json"))
    if candidates:
        return candidates[0]
    return None

# Map site_url / country_code to readable market names
DOMAIN_MARKET_MAP = {
    "ae": {"domain": "woodupp.ae", "market": "UAE", "flag": "\ud83c\udde6\ud83c\uddea"},
    "at": {"domain": "woodupp.at", "market": "Austria", "flag": "\ud83c\udde6\ud83c\uddf9"},
    "au": {"domain": "woodupp.au", "market": "Australia", "flag": "\ud83c\udde6\ud83c\uddfa"},
    "be": {"domain": "woodupp.be", "market": "Belgium", "flag": "\ud83c\udde7\ud83c\uddea"},
    "ch": {"domain": "woodupp.ch", "market": "Switzerland", "flag": "\ud83c\udde8\ud83c\udded"},
    "couk": {"domain": "woodupp.co.uk", "market": "United Kingdom", "flag": "\ud83c\uddec\ud83c\udde7"},
    "coza": {"domain": "woodupp.co.za", "market": "South Africa", "flag": "\ud83c\uddff\ud83c\udde6"},
    "com_na": {"domain": "woodupp.com.na", "market": "Namibia", "flag": "\ud83c\uddf3\ud83c\udde6"},
    "com": {"domain": "woodupp.com", "market": "Global (.com)", "flag": "\ud83c\udf10"},
    "us": {"domain": "woodupp.com/us", "market": "USA", "flag": "\ud83c\uddfa\ud83c\uddf8"},
    "de": {"domain": "woodupp.de", "market": "Germany", "flag": "\ud83c\udde9\ud83c\uddea"},
    "dk": {"domain": "woodupp.dk", "market": "Denmark", "flag": "\ud83c\udde9\ud83c\uddf0"},
    "es": {"domain": "woodupp.es", "market": "Spain", "flag": "\ud83c\uddea\ud83c\uddf8"},
    "fr": {"domain": "woodupp.fr", "market": "France", "flag": "\ud83c\uddeb\ud83c\uddf7"},
    "it": {"domain": "woodupp.it", "market": "Italy", "flag": "\ud83c\uddee\ud83c\uddf9"},
    "nl": {"domain": "woodupp.nl", "market": "Netherlands", "flag": "\ud83c\uddf3\ud83c\uddf1"},
    "no": {"domain": "woodupp.no", "market": "Norway", "flag": "\ud83c\uddf3\ud83c\uddf4"},
    "pl": {"domain": "woodupp.pl", "market": "Poland", "flag": "\ud83c\uddf5\ud83c\uddf1"},
    "pt": {"domain": "woodupp.pt", "market": "Portugal", "flag": "\ud83c\uddf5\ud83c\uddf9"},
    "se": {"domain": "woodupp.se", "market": "Sweden", "flag": "\ud83c\uddf8\ud83c\uddea"},
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
    "uzb": "Uzbekistan", "prt": "Portugal", "xkk": "Kosovo",
}


def load_and_clean():
    print("Loading CSV...")
    df = pd.read_csv(SOURCE_CSV, dtype={
        "is_anonymized_query": str,
        "is_anonymized_discover": str,
        "impressions": int,
        "clicks": int,
    })
    print(f"  Loaded {len(df):,} rows")

    # Convert types
    df["data_date"] = pd.to_datetime(df["data_date"]).dt.strftime("%Y-%m-%d")
    df["is_anonymized_query"] = df["is_anonymized_query"].str.lower() == "true"
    df["is_anonymized_discover"] = df["is_anonymized_discover"].str.lower() == "true"

    # Add market label from country_code
    df["market"] = df["country_code"].map(lambda x: DOMAIN_MARKET_MAP.get(x, {}).get("market", x))
    df["domain"] = df["country_code"].map(lambda x: DOMAIN_MARKET_MAP.get(x, {}).get("domain", x))

    # Visitor country readable name
    df["visitor_country"] = df["country"].map(lambda x: COUNTRY_ISO_MAP.get(x, x))

    # Average position (sum_position / impressions)
    df["avg_position"] = (df["sum_position"] / df["impressions"]).round(1)
    df.loc[df["impressions"] == 0, "avg_position"] = 0

    # CTR
    df["ctr"] = (df["clicks"] / df["impressions"] * 100).round(2)
    df.loc[df["impressions"] == 0, "ctr"] = 0

    # URL path (strip domain)
    df["url_path"] = df["url"].apply(lambda u: "/" + "/".join(u.split("/")[3:]) if isinstance(u, str) and len(u.split("/")) > 3 else "/")

    print(f"  Date range: {df['data_date'].min()} to {df['data_date'].max()}")
    print(f"  Markets: {df['market'].nunique()}")
    print(f"  Visitor countries: {df['visitor_country'].nunique()}")

    return df


def generate_overview(df):
    """Global KPIs and summary stats."""
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
    """Daily aggregated metrics by market."""
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

    # Also aggregate "All Markets" totals per day
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
    """Anonymized query analysis by market, country, date, device, search_type."""
    # By market and date
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

    # By visitor country
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

    # By device
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

    # By search type
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
    """Top URLs by impressions/clicks per market."""
    # Filter to non-anonymized for richer data, but aggregate all
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

    # All markets combined
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
    """Top keywords by impressions/clicks per market (non-anonymized only)."""
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

    # All markets
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
    """Visitor country analysis - where impressions/clicks come from."""
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

    # Country by market breakdown
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

    # Daily by top visitor countries
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
    """Device and search type breakdowns."""
    # By market and device
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

    # By market and search type
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

    # Daily by device
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

    # Daily by search type
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
    """SERP feature analysis."""
    feature_cols = [c for c in df.columns if c.startswith("is_") and c not in ["is_anonymized_query", "is_anonymized_discover"]]

    # Aggregate per feature across all data
    feature_summary = {}
    for col in feature_cols:
        # Handle mixed types - convert to boolean properly
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

    # Per market
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
    """Daily performance for top URLs across all markets."""
    # Get top 50 URLs overall
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
    """Daily performance for top keywords."""
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


# ═════════════════════════════════════════════════════════════
# Brand vs Non-Brand classification
# ═════════════════════════════════════════════════════════════

# Matches woodupp, woodup, wood-upp, wood upp, etc.
BRAND_REGEX = re.compile(r"wood\s*-?\s*up", re.IGNORECASE)


def classify_query(q):
    if not isinstance(q, str) or not q:
        return None
    return "brand" if BRAND_REGEX.search(q) else "nonbrand"


def _agg_brand_segment(seg_df):
    if seg_df is None or seg_df.empty:
        return {"impressions": 0, "clicks": 0, "queries": 0, "avg_position": 0, "ctr": 0}
    imp = int(seg_df["impressions"].sum())
    clk = int(seg_df["clicks"].sum())
    pos_sum = float(seg_df["sum_position"].sum())
    return {
        "impressions": imp,
        "clicks": clk,
        "queries": int(seg_df["query"].nunique()),
        "avg_position": round(pos_sum / imp, 1) if imp > 0 else 0,
        "ctr": round(clk / imp * 100, 2) if imp > 0 else 0,
    }


def generate_brand_analysis(df):
    """Brand vs non-brand search insights.

    Brand share is computed only over *known* (non-anonymized) queries. The
    anonymized share is reported separately as context — it cannot be
    classified, since Google hides the actual query text.
    """
    known = df[(~df["is_anonymized_query"]) & df["query"].notna() & (df["query"] != "")].copy()
    known["segment"] = known["query"].apply(classify_query)
    known = known[known["segment"].notna()]
    anon = df[df["is_anonymized_query"]]

    brand_df = known[known["segment"] == "brand"]
    nonbrand_df = known[known["segment"] == "nonbrand"]

    overall = {
        "brand": _agg_brand_segment(brand_df),
        "nonbrand": _agg_brand_segment(nonbrand_df),
        "anonymized": {
            "impressions": int(anon["impressions"].sum()),
            "clicks": int(anon["clicks"].sum()),
        },
    }

    by_market = {}
    for market in sorted(known["market"].unique()):
        mdf = known[known["market"] == market]
        manon = anon[anon["market"] == market]
        by_market[market] = {
            "brand": _agg_brand_segment(mdf[mdf["segment"] == "brand"]),
            "nonbrand": _agg_brand_segment(mdf[mdf["segment"] == "nonbrand"]),
            "anonymized": {
                "impressions": int(manon["impressions"].sum()),
                "clicks": int(manon["clicks"].sum()),
            },
        }

    # Daily series (per-market + All Markets) for trend charts and comparisons
    daily = {}

    daily_market = known.groupby(["data_date", "market", "segment"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()
    for _, row in daily_market.iterrows():
        d, m, seg = row["data_date"], row["market"], row["segment"]
        if d not in daily:
            daily[d] = {}
        if m not in daily[d]:
            daily[d][m] = {"brand": {"impressions": 0, "clicks": 0}, "nonbrand": {"impressions": 0, "clicks": 0}}
        daily[d][m][seg] = {"impressions": int(row["impressions"]), "clicks": int(row["clicks"])}

    daily_all = known.groupby(["data_date", "segment"]).agg(
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
    ).reset_index()
    for _, row in daily_all.iterrows():
        d, seg = row["data_date"], row["segment"]
        if d not in daily:
            daily[d] = {}
        if "All Markets" not in daily[d]:
            daily[d]["All Markets"] = {"brand": {"impressions": 0, "clicks": 0}, "nonbrand": {"impressions": 0, "clicks": 0}}
        daily[d]["All Markets"][seg] = {"impressions": int(row["impressions"]), "clicks": int(row["clicks"])}

    def _top_queries(seg_df, n=30):
        if seg_df.empty:
            return []
        agg = seg_df.groupby("query").agg(
            impressions=("impressions", "sum"),
            clicks=("clicks", "sum"),
            sum_position=("sum_position", "sum"),
            market_count=("market", "nunique"),
        ).reset_index()
        agg["avg_position"] = (agg["sum_position"] / agg["impressions"]).round(1)
        agg["ctr"] = (agg["clicks"] / agg["impressions"] * 100).round(2)
        top = agg.nlargest(n, "clicks")
        return [{
            "query": row["query"],
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "avg_position": float(row["avg_position"]) if pd.notna(row["avg_position"]) else 0,
            "ctr": float(row["ctr"]) if pd.notna(row["ctr"]) else 0,
            "markets": int(row["market_count"]),
        } for _, row in top.iterrows()]

    return {
        "overall": overall,
        "by_market": by_market,
        "daily": daily,
        "top_brand": _top_queries(brand_df, 30),
        "top_nonbrand": _top_queries(nonbrand_df, 30),
        "brand_pattern": "wood\\s*-?\\s*up (case-insensitive)",
    }


# ═════════════════════════════════════════════════════════════
# GA4 / BigQuery integration
# ═════════════════════════════════════════════════════════════

# Map GA4 account_name suffix (2-letter code) to the GSC market key used
# elsewhere in the dashboard. Kept in sync with DOMAIN_MARKET_MAP's markets.
GA4_ACCOUNT_TO_MARKET = {
    "AE": "UAE",
    "AT": "Austria",
    "AU": "Australia",
    "BE": "Belgium",
    "CH": "Switzerland",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "ZA": "South Africa",
    "COM": "Global (.com)",
    "EU": "Global (.com)",  # "GA4 - WoodUpp COM (EU)" is treated as the global .com market
    "US": "USA",
    "USA": "USA",
    "DE": "Germany",
    "DK": "Denmark",
    "ES": "Spain",
    "FR": "France",
    "IT": "Italy",
    "NL": "Netherlands",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "SE": "Sweden",
}

# Per-market local currency (best-effort). Used only for display in the UI.
MARKET_CURRENCY = {
    "UAE": "AED",
    "Austria": "EUR",
    "Australia": "AUD",
    "Belgium": "EUR",
    "Switzerland": "CHF",
    "United Kingdom": "GBP",
    "South Africa": "ZAR",
    "Global (.com)": "EUR",
    "USA": "USD",
    "Germany": "EUR",
    "Denmark": "DKK",
    "Spain": "EUR",
    "France": "EUR",
    "Italy": "EUR",
    "Netherlands": "EUR",
    "Norway": "NOK",
    "Poland": "PLN",
    "Portugal": "EUR",
    "Sweden": "SEK",
}


def parse_ga4_account_name(name):
    """Extract the market name from a GA4 account_name string.

    Handles trailing flag emojis, extra whitespace, and parenthetical codes.

    Examples handled:
        "GA4 - WoodUpp DE \U0001F1E9\U0001F1EA "    -> "Germany"
        "GA4 - WoodUpp UK \U0001F1EC\U0001F1E7 "    -> "United Kingdom"
        "GA4 - WoodUpp COM (EU)"                    -> "Global (.com)"
        "GA4 - WoodUpp USA \U0001F1FA\U0001F1F8 "   -> "USA"
    """
    if not isinstance(name, str):
        return None
    # 1) Prefer parenthetical code if present: "COM (EU)" -> "EU"
    paren = re.search(r"\(([A-Za-z]{2,3})\)", name)
    if paren:
        code = paren.group(1).upper()
        if code in GA4_ACCOUNT_TO_MARKET:
            return GA4_ACCOUNT_TO_MARKET[code]

    # 2) Strip parenthetical groups, then find every 2-3 letter uppercase
    #    ASCII token (flag emojis and lowercase words are ignored). Pick
    #    the last one that exists in the mapping. "GA" from "GA4" never
    #    matches because \b requires a word boundary after, and "4" is a
    #    word char — so it's safely excluded.
    cleaned = re.sub(r"\s*\([^)]*\)", "", name)
    codes = re.findall(r"\b([A-Z]{2,3})\b", cleaned)
    for code in reversed(codes):
        if code in GA4_ACCOUNT_TO_MARKET:
            return GA4_ACCOUNT_TO_MARKET[code]
    return None


def load_ga4_from_bigquery():
    """Query GA4 organic search data from BigQuery for the last N years.

    Returns a pandas DataFrame with columns: date, market, sessions,
    totalusers, conversions, totalrevenue. Returns None if the BigQuery
    client or credentials are unavailable.
    """
    creds_path = find_ga4_credentials()
    if not creds_path:
        print("  [GA4] No service account credentials found — skipping BigQuery step.")
        print("        Place obsidian-*.json one directory above the project folder,")
        print("        or set GOOGLE_APPLICATION_CREDENTIALS.")
        return None

    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError:
        print("  [GA4] google-cloud-bigquery not installed — skipping.")
        print("        Install with: pip install google-cloud-bigquery")
        return None

    print(f"  [GA4] Using credentials: {creds_path}")
    credentials = service_account.Credentials.from_service_account_file(
        str(creds_path),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=GA4_PROJECT_ID)

    cutoff = (date.today() - timedelta(days=GA4_YEARS_BACK * 365)).isoformat()
    query = f"""
        SELECT
            DATE(date) AS date,
            account_name,
            SUM(sessions) AS sessions,
            SUM(totalusers) AS totalusers,
            SUM(conversions) AS conversions,
            SUM(totalrevenue) AS totalrevenue
        FROM `{GA4_TABLE}`
        WHERE session_default_channel_group = 'Organic Search'
          AND DATE(date) >= DATE('{cutoff}')
        GROUP BY date, account_name
        ORDER BY date
    """
    print(f"  [GA4] Querying {GA4_TABLE} since {cutoff}...")
    df = client.query(query).to_dataframe()
    print(f"  [GA4] Retrieved {len(df):,} rows")

    if df.empty:
        return df

    df["market"] = df["account_name"].map(parse_ga4_account_name)
    unmapped = df[df["market"].isna()]["account_name"].unique()
    if len(unmapped):
        print(f"  [GA4] Warning: unmapped account_names: {list(unmapped)}")
    df = df.dropna(subset=["market"])

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for col in ["sessions", "totalusers", "conversions"]:
        df[col] = df[col].fillna(0).astype(int)
    df["totalrevenue"] = df["totalrevenue"].fillna(0).astype(float).round(2)

    # Collapse duplicates (e.g. COM + EU → Global (.com)) by summing
    df = df.groupby(["date", "market"], as_index=False).agg(
        sessions=("sessions", "sum"),
        totalusers=("totalusers", "sum"),
        conversions=("conversions", "sum"),
        totalrevenue=("totalrevenue", "sum"),
    )
    return df


def generate_ga4_data(ga_df):
    """Build the ga4.json payload from the GA4 DataFrame.

    Structure:
        {
            "has_data": bool,
            "date_range": {"start": "...", "end": "..."},
            "markets": ["Germany", ...],
            "currencies": {"Germany": "EUR", ...},
            "daily": {
                "YYYY-MM-DD": {
                    "Germany":   {"sessions":..., "users":..., "conversions":..., "revenue":...},
                    "All Markets": {...}
                }
            },
            "totals_per_market": {
                "Germany": {"sessions":..., "users":..., "conversions":..., "revenue":...}
            },
            "totals_all": {"sessions":..., "users":..., "conversions":..., "revenue":...}
        }
    """
    if ga_df is None or ga_df.empty:
        return {"has_data": False}

    markets = sorted(ga_df["market"].unique().tolist())
    dates = sorted(ga_df["date"].unique().tolist())

    daily = {}
    for _, row in ga_df.iterrows():
        d = row["date"]
        if d not in daily:
            daily[d] = {}
        daily[d][row["market"]] = {
            "sessions": int(row["sessions"]),
            "users": int(row["totalusers"]),
            "conversions": int(row["conversions"]),
            "revenue": float(round(row["totalrevenue"], 2)),
        }

    # Daily "All Markets" totals (revenue intentionally summed as mixed-currency —
    # UI should display per-market revenue, not the combined number.)
    all_daily = ga_df.groupby("date", as_index=False).agg(
        sessions=("sessions", "sum"),
        totalusers=("totalusers", "sum"),
        conversions=("conversions", "sum"),
    )
    for _, row in all_daily.iterrows():
        daily[row["date"]]["All Markets"] = {
            "sessions": int(row["sessions"]),
            "users": int(row["totalusers"]),
            "conversions": int(row["conversions"]),
            "revenue": 0,  # mixed currency — not meaningful to sum
        }

    totals_per_market = {}
    for m in markets:
        mdf = ga_df[ga_df["market"] == m]
        totals_per_market[m] = {
            "sessions": int(mdf["sessions"].sum()),
            "users": int(mdf["totalusers"].sum()),
            "conversions": int(mdf["conversions"].sum()),
            "revenue": float(round(mdf["totalrevenue"].sum(), 2)),
            "currency": MARKET_CURRENCY.get(m, ""),
        }

    totals_all = {
        "sessions": int(ga_df["sessions"].sum()),
        "users": int(ga_df["totalusers"].sum()),
        "conversions": int(ga_df["conversions"].sum()),
    }

    return {
        "has_data": True,
        "date_range": {"start": dates[0], "end": dates[-1]},
        "dates": dates,
        "markets": markets,
        "currencies": {m: MARKET_CURRENCY.get(m, "") for m in markets},
        "daily": daily,
        "totals_per_market": totals_per_market,
        "totals_all": totals_all,
    }


def main():
    df = load_and_clean()

    print("\nGenerating overview...")
    overview = generate_overview(df)
    with open(DATA_DIR / "overview.json", "w") as f:
        json.dump(overview, f)
    print(f"  Saved overview.json")

    print("Generating daily metrics...")
    daily = generate_daily_metrics(df)
    with open(DATA_DIR / "daily_metrics.json", "w") as f:
        json.dump(daily, f)
    print(f"  Saved daily_metrics.json")

    print("Generating anonymized query data...")
    anon = generate_anonymized_data(df)
    with open(DATA_DIR / "anonymized.json", "w") as f:
        json.dump(anon, f)
    print(f"  Saved anonymized.json")

    print("Generating URL performance...")
    urls = generate_url_performance(df)
    with open(DATA_DIR / "url_performance.json", "w") as f:
        json.dump(urls, f)
    print(f"  Saved url_performance.json")

    print("Generating keyword performance...")
    keywords = generate_keyword_performance(df)
    with open(DATA_DIR / "keyword_performance.json", "w") as f:
        json.dump(keywords, f)
    print(f"  Saved keyword_performance.json")

    print("Generating country data...")
    countries = generate_country_data(df)
    with open(DATA_DIR / "country_data.json", "w") as f:
        json.dump(countries, f)
    print(f"  Saved country_data.json")

    print("Generating device & search type data...")
    device = generate_device_search_data(df)
    with open(DATA_DIR / "device_search.json", "w") as f:
        json.dump(device, f)
    print(f"  Saved device_search.json")

    print("Generating SERP features data...")
    features = generate_search_features(df)
    with open(DATA_DIR / "serp_features.json", "w") as f:
        json.dump(features, f)
    print(f"  Saved serp_features.json")

    print("Generating URL daily trends...")
    url_daily = generate_url_daily(df)
    with open(DATA_DIR / "url_daily.json", "w") as f:
        json.dump(url_daily, f)
    print(f"  Saved url_daily.json")

    print("Generating keyword daily trends...")
    kw_daily = generate_keyword_daily(df)
    with open(DATA_DIR / "keyword_daily.json", "w") as f:
        json.dump(kw_daily, f)
    print(f"  Saved keyword_daily.json")

    print("Generating brand vs non-brand analysis...")
    brand = generate_brand_analysis(df)
    with open(DATA_DIR / "brand_analysis.json", "w") as f:
        json.dump(brand, f)
    o = brand["overall"]
    bk = o["brand"]["clicks"]; nb = o["nonbrand"]["clicks"]; tot = bk + nb
    bshare = (bk/tot*100) if tot > 0 else 0
    print(f"  Saved brand_analysis.json — brand clicks share (of known): {bshare:.1f}%")

    print("\nPulling GA4 data from BigQuery...")
    try:
        ga_df = load_ga4_from_bigquery()
        ga_payload = generate_ga4_data(ga_df)
    except Exception as e:
        print(f"  [GA4] Error fetching GA4 data: {e}")
        print(f"  [GA4] Continuing without GA4 data.")
        ga_payload = {"has_data": False, "error": str(e)}
    with open(DATA_DIR / "ga4.json", "w") as f:
        json.dump(ga_payload, f)
    if ga_payload.get("has_data"):
        print(f"  Saved ga4.json — {len(ga_payload.get('dates', []))} days, "
              f"{len(ga_payload.get('markets', []))} markets")
    else:
        print(f"  Saved ga4.json (empty placeholder — will be picked up once credentials are present)")

    print("\nDone! All data files saved to", DATA_DIR)


if __name__ == "__main__":
    main()
