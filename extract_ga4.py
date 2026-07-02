"""
Extracts GA4 analytics data from BigQuery and saves to:
  data/ga4_data.json  — Analytics tab (monthly + last 90 days daily, organic traffic)
  data/ga4.json       — Dashboard tab (2-year daily per-market conversions/revenue)
  data/ai_traffic.json — AI Traffic tab (monthly + daily, medium='ai-assistant')

Data sources:
  ga4_v3_sessions — per-account daily sessions by source/medium
  ga4_v3_revenue  — per-account daily total revenue & purchase conversions
  ga4_events      — per-account event counts

Requires: google-cloud-bigquery  (pip install google-cloud-bigquery)
Auth:      set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`

Usage:
    python extract_ga4.py
"""
import json
from pathlib import Path
from google.cloud import bigquery

PROJECT_ID = "obsidian-375910"
SESSIONS_TABLE = f"{PROJECT_ID}.woodupp.ga4_v3_sessions"
REVENUE_TABLE = f"{PROJECT_ID}.woodupp.ga4_v3_revenue"
EVENTS_TABLE = f"{PROJECT_ID}.woodupp.ga4_events"
OUTPUT_PATH = Path(__file__).parent / "data" / "ga4_data.json"
AGG_OUTPUT_PATH = Path(__file__).parent / "data" / "ga4.json"
AI_OUTPUT_PATH = Path(__file__).parent / "data" / "ai_traffic.json"

CURRENCY_MAP = {
    "Australia": "AUD",
    "Austria": "EUR",
    "Belgium": "EUR",
    "Denmark": "DKK",
    "France": "EUR",
    "Germany": "EUR",
    "Global (.com)": "EUR",
    "Italy": "EUR",
    "Netherlands": "EUR",
    "Norway": "NOK",
    "Poland": "PLN",
    "Portugal": "EUR",
    "South Africa": "ZAR",
    "Spain": "EUR",
    "Sweden": "SEK",
    "Switzerland": "CHF",
    "UAE": "AED",
    "USA": "USD",
    "United Kingdom": "GBP",
}

ACCOUNT_MAP = {
    "GA4 - WoodUpp AE \U0001f1e6\U0001f1ea": "UAE",
    "GA4 - WoodUpp AT \U0001f1e6\U0001f1f9": "Austria",
    "GA4 - WoodUpp AU \U0001f1e6\U0001f1fa": "Australia",
    "GA4 - WoodUpp BE \U0001f1e7\U0001f1ea": "Belgium",
    "GA4 - WoodUpp CH \U0001f1e8\U0001f1ed": "Switzerland",
    "GA4 - WoodUpp COM (EU) \U0001f1ea\U0001f1fa": "Global (.com)",
    "GA4 - WoodUpp DE \U0001f1e9\U0001f1ea": "Germany",
    "GA4 - WoodUpp DK \U0001f1e9\U0001f1f0": "Denmark",
    "GA4 - WoodUpp ES \U0001f1ea\U0001f1f8": "Spain",
    "GA4 - WoodUpp FR \U0001f1eb\U0001f1f7": "France",
    "GA4 - WoodUpp IT \U0001f1ee\U0001f1f9": "Italy",
    "GA4 - WoodUpp NL \U0001f1f3\U0001f1f1": "Netherlands",
    "GA4 - WoodUpp NO \U0001f1f3\U0001f1f4": "Norway",
    "GA4 - WoodUpp PL \U0001f1f5\U0001f1f1": "Poland",
    "GA4 - WoodUpp PT \U0001f1f5\U0001f1f9": "Portugal",
    "GA4 - WoodUpp SE \U0001f1f8\U0001f1ea": "Sweden",
    "GA4 - WoodUpp UK \U0001f1ec\U0001f1e7": "United Kingdom",
    "GA4 - WoodUpp USA \U0001f1fa\U0001f1f8": "USA",
    "GA4 - WoodUpp ZA \U0001f1ff\U0001f1e6": "South Africa",
}


def query_revenue_by_date(client, date_filter=""):
    """Query revenue table and return dict: {(market, date_str): {revenue, conversions}}."""
    where = f"WHERE {date_filter}" if date_filter else ""
    q = f"""
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(totalrevenue AS FLOAT64)) AS revenue,
            SUM(CAST(conversions_purchase AS INT64)) AS conversions
        FROM `{REVENUE_TABLE}`
        {where}
        GROUP BY account_name, date
    """
    result = {}
    for row in client.query(q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        result[(market, row.date)] = {
            "revenue": round(float(row.revenue), 2),
            "conversions": row.conversions,
        }
    return result


def build_ga4_agg(client):
    """Build ga4.json: 2-year daily per-market data for the Dashboard tab."""
    print("Querying full daily history for ga4.json...")

    sessions_q = f"""
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(sessions AS INT64)) AS sessions
        FROM `{SESSIONS_TABLE}`
        WHERE SPLIT(source_medium, ' / ')[SAFE_OFFSET(1)] = 'organic'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
        GROUP BY account_name, date
        ORDER BY date, account_name
    """
    rev = query_revenue_by_date(client, "date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)")

    daily = {}
    totals = {}
    for row in client.query(sessions_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        r = rev.get((market, row.date), {"revenue": 0.0, "conversions": 0})
        daily.setdefault(row.date, {})[market] = {
            "sessions": row.sessions,
            "users": 0,
            "conversions": r["conversions"],
            "revenue": r["revenue"],
        }
        t = totals.setdefault(market, {"sessions": 0, "users": 0, "conversions": 0, "revenue": 0.0})
        t["sessions"] += row.sessions
        t["conversions"] += r["conversions"]
        t["revenue"] = round(t["revenue"] + r["revenue"], 2)

    for market, t in totals.items():
        t["currency"] = CURRENCY_MAP.get(market, "EUR")

    all_dates = sorted(daily.keys())
    all_markets = sorted(totals.keys())
    totals_all = {
        "sessions": sum(t["sessions"] for t in totals.values()),
        "users": sum(t["users"] for t in totals.values()),
        "conversions": sum(t["conversions"] for t in totals.values()),
    }

    agg_data = {
        "has_data": bool(daily),
        "date_range": {
            "start": all_dates[0] if all_dates else "",
            "end": all_dates[-1] if all_dates else "",
        },
        "dates": all_dates,
        "markets": all_markets,
        "currencies": {m: CURRENCY_MAP.get(m, "EUR") for m in all_markets},
        "daily": daily,
        "totals_per_market": totals,
        "totals_all": totals_all,
    }

    AGG_OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(AGG_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(agg_data, f, ensure_ascii=False)

    import os
    size_mb = os.path.getsize(AGG_OUTPUT_PATH) / (1024 * 1024)
    print(f"  {len(all_dates)} dates, {len(all_markets)} markets")
    print(f"  Saved {AGG_OUTPUT_PATH} ({size_mb:.2f} MB)")


def build_ai_traffic(client):
    """Build ai_traffic.json: daily + monthly AI-assistant referred traffic."""
    print("\nQuerying AI traffic (medium='ai-assistant')...")

    monthly_q = f"""
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            SPLIT(source_medium, ' / ')[SAFE_OFFSET(0)] AS source,
            SUM(CAST(sessions AS INT64)) AS sessions
        FROM `{SESSIONS_TABLE}`
        WHERE SPLIT(source_medium, ' / ')[SAFE_OFFSET(1)] = 'ai-assistant'
        GROUP BY account_name, month, source
        ORDER BY account_name, month, source
    """
    monthly = {}
    sources = set()
    for row in client.query(monthly_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        monthly.setdefault(market, {})
        monthly[market].setdefault(row.month, {"sessions": 0, "conversions": 0, "revenue": 0.0, "by_source": {}})
        monthly[market][row.month]["sessions"] += row.sessions
        monthly[market][row.month]["by_source"][row.source] = {
            "sessions": row.sessions,
            "conversions": 0,
            "revenue": 0.0,
        }
        sources.add(row.source)
    print(f"  Monthly: {len(monthly)} markets, {len(sources)} AI sources: {sorted(sources)}")

    daily_q = f"""
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SPLIT(source_medium, ' / ')[SAFE_OFFSET(0)] AS source,
            SUM(CAST(sessions AS INT64)) AS sessions
        FROM `{SESSIONS_TABLE}`
        WHERE SPLIT(source_medium, ' / ')[SAFE_OFFSET(1)] = 'ai-assistant'
        GROUP BY account_name, date, source
        ORDER BY date, account_name, source
    """
    daily = {}
    for row in client.query(daily_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        daily.setdefault(row.date, {})
        if market not in daily[row.date]:
            daily[row.date][market] = {"sessions": 0, "conversions": 0, "revenue": 0.0, "by_source": {}}
        daily[row.date][market]["sessions"] += row.sessions
        daily[row.date][market]["by_source"][row.source] = {
            "sessions": row.sessions,
            "conversions": 0,
            "revenue": 0.0,
        }

    for date in daily:
        totals = {"sessions": 0, "conversions": 0, "revenue": 0.0}
        for m, d in daily[date].items():
            totals["sessions"] += d["sessions"]
        daily[date]["All Markets"] = totals

    all_months = sorted(set(m for mkt in monthly.values() for m in mkt))
    all_dates = sorted(daily.keys())

    ai_data = {
        "monthly": monthly,
        "daily": daily,
        "months": all_months,
        "sources": sorted(sources),
        "date_range": {
            "min": all_dates[0] if all_dates else "",
            "max": all_dates[-1] if all_dates else "",
        },
    }

    AI_OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(AI_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(ai_data, f, ensure_ascii=False)

    import os
    size_mb = os.path.getsize(AI_OUTPUT_PATH) / (1024 * 1024)
    print(f"  {len(all_dates)} daily dates, {len(all_months)} months")
    print(f"  Saved {AI_OUTPUT_PATH} ({size_mb:.2f} MB)")


def main():
    client = bigquery.Client(project=PROJECT_ID)

    # --- Monthly organic sessions + revenue ---
    print("Querying monthly sessions (organic)...")
    sessions_q = f"""
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            SUM(CAST(sessions AS INT64)) AS sessions
        FROM `{SESSIONS_TABLE}`
        WHERE SPLIT(source_medium, ' / ')[SAFE_OFFSET(1)] = 'organic'
        GROUP BY account_name, month
        ORDER BY account_name, month
    """
    print("Querying monthly revenue...")
    revenue_q = f"""
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue,
            SUM(CAST(conversions_purchase AS INT64)) AS conversions
        FROM `{REVENUE_TABLE}`
        GROUP BY account_name, month
        ORDER BY account_name, month
    """

    monthly_sessions = {}
    for row in client.query(sessions_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        monthly_sessions.setdefault(market, {})[row.month] = row.sessions

    monthly_revenue = {}
    for row in client.query(revenue_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        monthly_revenue.setdefault(market, {})[row.month] = {
            "revenue": float(row.revenue),
            "conversions": row.conversions,
        }

    monthly = {}
    all_markets = set(list(monthly_sessions.keys()) + list(monthly_revenue.keys()))
    for market in all_markets:
        monthly[market] = {}
        all_m = set(list(monthly_sessions.get(market, {}).keys()) + list(monthly_revenue.get(market, {}).keys()))
        for month in all_m:
            sess = monthly_sessions.get(market, {}).get(month, 0)
            rev = monthly_revenue.get(market, {}).get(month, {"revenue": 0.0, "conversions": 0})
            monthly[market][month] = {
                "sessions": sess,
                "users": 0,
                "conversions": rev["conversions"],
                "revenue": rev["revenue"],
            }
    print(f"  {len(monthly)} markets, {sum(len(v) for v in monthly.values())} month-market rows")

    # --- Events ---
    print("Querying all events (monthly)...")
    events_q = f"""
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            event_name,
            SUM(CAST(event_count AS INT64)) AS events
        FROM `{EVENTS_TABLE}`
        GROUP BY account_name, month, event_name
        ORDER BY account_name, month, event_name
    """
    events = {}
    event_names = set()
    for row in client.query(events_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        events.setdefault(market, {})
        events[market].setdefault(row.month, {})
        events[market][row.month][row.event_name] = row.events
        event_names.add(row.event_name)
    print(f"  {len(events)} markets, {len(event_names)} distinct events")

    # --- Daily organic sessions + revenue (last 90 days) ---
    print("Querying daily sessions (last 90 days, organic)...")
    daily_sessions_q = f"""
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(sessions AS INT64)) AS sessions
        FROM `{SESSIONS_TABLE}`
        WHERE SPLIT(source_medium, ' / ')[SAFE_OFFSET(1)] = 'organic'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY account_name, date
        ORDER BY account_name, date
    """
    rev_90d = query_revenue_by_date(client, "date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)")

    daily = {}
    for row in client.query(daily_sessions_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        r = rev_90d.get((market, row.date), {"revenue": 0.0, "conversions": 0})
        daily.setdefault(row.date, {})
        daily[row.date][market] = {
            "sessions": row.sessions,
            "users": 0,
            "conversions": r["conversions"],
            "revenue": r["revenue"],
        }

    for date in daily:
        totals = {"sessions": 0, "users": 0, "conversions": 0, "revenue": 0.0}
        for m, d in daily[date].items():
            for k in totals:
                totals[k] += d[k]
        totals["revenue"] = round(totals["revenue"], 2)
        daily[date]["All Markets"] = totals
    print(f"  {len(daily)} daily dates")

    all_months = sorted(set(m for mkt in monthly.values() for m in mkt))

    ga4_data = {
        "monthly": monthly,
        "events": events,
        "event_names": sorted(event_names),
        "daily": daily,
        "months": all_months,
        "date_range": {
            "min": min(daily.keys()) if daily else "",
            "max": max(daily.keys()) if daily else "",
        },
    }

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(ga4_data, f, ensure_ascii=False)

    import os
    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"\nSaved {OUTPUT_PATH} ({size_mb:.2f} MB)")
    if all_months:
        print(f"Months: {all_months[0]} to {all_months[-1]}")

    print("\nBuilding ga4.json (Dashboard tab)...")
    build_ga4_agg(client)

    build_ai_traffic(client)


if __name__ == "__main__":
    main()
