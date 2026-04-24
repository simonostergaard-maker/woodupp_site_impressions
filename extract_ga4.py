"""
Extracts GA4 analytics data from BigQuery and saves to data/ga4_data.json.

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


def main():
    client = bigquery.Client(project=PROJECT_ID)

    print("Querying monthly sessions/revenue...")
    monthly_q = """
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            SUM(CAST(sessions AS INT64)) AS sessions,
            SUM(CAST(totalusers AS INT64)) AS users,
            SUM(CAST(conversions AS INT64)) AS conversions,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue
        FROM `obsidian-375910.woodupp.ga4`
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
            "users": row.users,
            "conversions": row.conversions,
            "revenue": float(row.revenue),
        }
    print(f"  {len(monthly)} markets, {sum(len(v) for v in monthly.values())} month-market rows")

    print("Querying ecommerce funnel events (monthly)...")
    funnel_q = """
        SELECT
            account_name,
            FORMAT_DATE('%Y-%m', date) AS month,
            event_name,
            SUM(CAST(event_count AS INT64)) AS events,
            ROUND(SUM(CAST(event_value AS FLOAT64)), 2) AS value
        FROM `obsidian-375910.woodupp.ga4_events`
        WHERE event_name IN ('view_item', 'add_to_cart', 'begin_checkout', 'purchase')
        GROUP BY account_name, month, event_name
        ORDER BY account_name, month, event_name
    """
    funnel = {}
    for row in client.query(funnel_q):
        market = ACCOUNT_MAP.get(row.account_name)
        if not market:
            continue
        funnel.setdefault(market, {})
        funnel[market].setdefault(row.month, {})
        funnel[market][row.month][row.event_name] = {
            "events": row.events,
            "value": float(row.value),
        }
    print(f"  {len(funnel)} markets with funnel data")

    print("Querying daily sessions (last 90 days)...")
    daily_q = """
        SELECT
            account_name,
            CAST(date AS STRING) AS date,
            SUM(CAST(sessions AS INT64)) AS sessions,
            SUM(CAST(totalusers AS INT64)) AS users,
            SUM(CAST(conversions AS INT64)) AS conversions,
            ROUND(SUM(CAST(totalrevenue AS FLOAT64)), 2) AS revenue
        FROM `obsidian-375910.woodupp.ga4`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
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
            "users": row.users,
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
        "funnel": funnel,
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


if __name__ == "__main__":
    main()
