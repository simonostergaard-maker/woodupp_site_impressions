"""
Extracts Google Search Console data from BigQuery.

Pulls two tables:
  combined_url_impressions  -> data/woodupp_url_impressions.csv  (used by preprocess.py)
  combined_site_impressions -> data/gsc_site_data.json           (daily site-level totals)

Requires:
    pip install google-cloud-bigquery google-cloud-bigquery-storage pyarrow

Auth: set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`
"""
import json
import os
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

PROJECT_ID       = "obsidian-375910"
DATASET_ID       = "woodupp"
URL_TABLE        = f"{PROJECT_ID}.{DATASET_ID}.combined_url_impressions"
SITE_TABLE       = f"{PROJECT_ID}.{DATASET_ID}.combined_site_impressions"
URL_OUTPUT       = Path(__file__).parent / "data" / "woodupp_url_impressions.csv"
SITE_OUTPUT      = Path(__file__).parent / "data" / "gsc_site_data.json"

# GSC "search appearance" flags on combined_url_impressions (see
# bigquery_add_eu_market.sql for the full table schema these come from).
# preprocess.py's generate_serp_features() looks for columns starting with
# "is_" (other than the two anonymization flags below) — keep this list in
# sync with that table's columns if GSC ever adds new search-appearance types.
SERP_FEATURE_COLUMNS = [
    "is_amp_top_stories", "is_amp_blue_link", "is_job_listing", "is_job_details",
    "is_tpf_qa", "is_tpf_faq", "is_tpf_howto", "is_weblite", "is_action",
    "is_events_listing", "is_events_details", "is_forums",
    "is_search_appearance_android_app", "is_amp_story", "is_amp_image_result",
    "is_video", "is_organic_shopping", "is_review_snippet",
    "is_special_announcement", "is_recipe_feature", "is_recipe_rich_snippet",
    "is_subscribed_content", "is_page_experience", "is_practice_problems",
    "is_math_solvers", "is_translated_result", "is_edu_q_and_a",
    "is_product_snippets", "is_merchant_listings", "is_learning_videos",
]
_SERP_FEATURE_SELECT = ",\n    ".join(f"CAST({c} AS BOOL) AS {c}" for c in SERP_FEATURE_COLUMNS)

URL_QUERY = f"""
SELECT
    data_date,
    country_code,
    CAST(impressions AS INT64)           AS impressions,
    CAST(clicks AS INT64)                AS clicks,
    CAST(is_anonymized_query AS BOOL)    AS is_anonymized_query,
    CAST(is_anonymized_discover AS BOOL) AS is_anonymized_discover,
    CAST(sum_position AS FLOAT64)        AS sum_position,
    url,
    query,
    country,
    device,
    search_type,
    {_SERP_FEATURE_SELECT}
FROM `{URL_TABLE}`
ORDER BY data_date, country_code
"""

SITE_QUERY = f"""
SELECT *
FROM `{SITE_TABLE}`
ORDER BY data_date
"""


def fetch_url_impressions(client):
    print(f"\n[1/2] Querying {URL_TABLE}...")
    print("      (large table — this may take several minutes)")
    try:
        df = client.query(URL_QUERY).to_dataframe()
    except NotFound:
        print(f"ERROR: Table '{URL_TABLE}' not found.")
        raise SystemExit(1)

    print(f"  {len(df):,} rows | {df['data_date'].min()} to {df['data_date'].max()}")
    URL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(URL_OUTPUT, index=False)
    mb = os.path.getsize(URL_OUTPUT) / 1024 / 1024
    print(f"  Saved {URL_OUTPUT} ({mb:.1f} MB)")


def fetch_site_impressions(client):
    print(f"\n[2/2] Querying {SITE_TABLE}...")
    try:
        df = client.query(SITE_QUERY).to_dataframe()
    except NotFound:
        print(f"  WARNING: Table '{SITE_TABLE}' not found — skipping site data.")
        return

    print(f"  {len(df):,} rows | {df['data_date'].min()} to {df['data_date'].max()}")

    # Convert to JSON: { date: { country_code: { impressions, clicks, ... } } }
    records = {}
    for _, row in df.iterrows():
        date = str(row["data_date"])[:10]
        market = str(row.get("country_code", row.get("site_url", "unknown")))
        records.setdefault(date, {})[market] = {
            k: (int(v) if hasattr(v, "item") and isinstance(v.item(), int)
                else float(v) if hasattr(v, "item") else v)
            for k, v in row.items()
            if k not in ("data_date", "country_code", "site_url")
        }

    SITE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(SITE_OUTPUT, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)
    mb = os.path.getsize(SITE_OUTPUT) / 1024 / 1024
    print(f"  Saved {SITE_OUTPUT} ({mb:.2f} MB)")


def main():
    print("Connecting to BigQuery project 'obsidian-375910'...")
    client = bigquery.Client(project=PROJECT_ID)

    fetch_url_impressions(client)
    fetch_site_impressions(client)

    print("\nDone! Run preprocess.py next to rebuild the dashboard.")


if __name__ == "__main__":
    main()
