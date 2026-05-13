"""
Extracts GA4 analytics data from BigQuery and saves to:
  data/ga4_data.json  — Analytics tab (monthly + last 90 days daily)
  data/ga4.json       — Dashboard tab (2-year daily per-market conversions/revenue)

Requires: google-cloud-bigquery  (pip install google-cloud-bigquery)
Auth:      set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`

Usage:
    python extract_ga4.py
"""
import json
from pathlib import Path
from google.cloud import bigquery

PROJECT_ID = "obsidian-375910"
OUTPUT_PATH = Path(__file__).parent / "data" / "ga4_data.json"
AGG_OUTPUT_PATH = Path(__file__).parent / "data" / "ga4.json"

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


def build_ga4_agg(client):
    """Build ga4.json: 2-year daily per-market data for the Dashboard tab."""
    print("Querying full daily history for ga4.json...")
    q = """
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(sessions AS INT64)) AS sessions,
            SUM(CAST(transactions AS INT64)) AS conversions,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue
        FROM `obsidian-375910.woodupp.ga4_v2`
        WHERE session_default_channel_group = 'Organic Search'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
        GROUP BY account_name, date
        ORDER BY date, account_name
    """

    daily = {}
    totals = {}
    for row in client.query(q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        daily.setdefault(row.date, {})[market] = {
            "sessions": row.sessions,
            "users": 0,
            "conversions": row.conversions,
            "revenue": float(row.revenue),
        }
        t = totals.setdefault(market, {"sessions": 0, "users": 0, "conversions": 0, "revenue": 0.0})
        t["sessions"] += row.sessions
        t["conversions"] += row.conversions
        t["revenue"] = round(t["revenue"] + float(row.revenue), 2)

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


def main():
    client = bigquery.Client(project=PROJECT_ID)

    print("Querying monthly sessions/revenue...")
    monthly_q = """
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            SUM(CAST(sessions AS INT64)) AS sessions,
            SUM(CAST(transactions AS INT64)) AS conversions,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue
        FROM `obsidian-375910.woodupp.ga4_v2`
        WHERE session_default_channel_group = 'Organic Search'
        GROUP BY account_name, month
        ORDER BY account_name, month
    """
    monthly = {}
    for row in client.query(monthly_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        monthly.setdefault(market, {})
        monthly[market][row.month] = {
            "sessions": row.sessions,
            "users": 0,
            "conversions": row.conversions,
            "revenue": float(row.revenue),
        }
    print(f"  {len(monthly)} markets, {sum(len(v) for v in monthly.values())} month-market rows")

    print("Querying all events (monthly)...")
    events_q = """
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            event_name,
            SUM(CAST(event_count AS INT64)) AS events
        FROM `obsidian-375910.woodupp.ga4_events`
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

    print("Querying daily sessions (last 90 days)...")
    daily_q = """
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(sessions AS INT64)) AS sessions,
            SUM(CAST(transactions AS INT64)) AS conversions,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue
        FROM `obsidian-375910.woodupp.ga4_v2`
        WHERE session_default_channel_group = 'Organic Search'
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY account_name, date
        ORDER BY account_name, date
    """
    daily = {}
    for row in client.query(daily_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        daily.setdefault(row.date, {})
        daily[row.date][market] = {
            "sessions": row.sessions,
            "users": 0,
            "conversions": row.conversions,
            "revenue": float(row.revenue),
        }

    # Add All Markets totals
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
    print(f"Months: {all_months[0]} to {all_months[-1]}")

    print("\nBuilding ga4.json (Dashboard tab)...")
    build_ga4_agg(client)


if __name__ == "__main__":
    main()
