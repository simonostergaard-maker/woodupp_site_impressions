"""
Extracts Google Search Console data from BigQuery and saves to a CSV file
that preprocess.py can consume.

Requires: google-cloud-bigquery  (pip install google-cloud-bigquery)
Auth:      set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`

Usage:
    python extract_gsc.py
    python extract_gsc.py --out C:/path/to/output.csv
"""
import argparse
import os
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

PROJECT_ID  = "obsidian-375910"
DATASET_ID  = "woodupp"
TABLE_ID    = f"{PROJECT_ID}.{DATASET_ID}.combined_url_impressions"
OUTPUT_PATH = Path(__file__).parent / "data" / "woodupp_url_impressions.csv"


QUERY = f"""
SELECT
    data_date,
    country_code,
    CAST(impressions AS INT64)         AS impressions,
    CAST(clicks AS INT64)              AS clicks,
    CAST(is_anonymized_query AS BOOL)  AS is_anonymized_query,
    CAST(is_anonymized_discover AS BOOL) AS is_anonymized_discover,
    CAST(sum_position AS FLOAT64)      AS sum_position,
    url,
    query,
    country,
    device,
    search_type
FROM `{TABLE_ID}`
ORDER BY data_date, country_code
"""


def list_tables(client):
    print(f"\nAvailable tables in {PROJECT_ID}.{DATASET_ID}:")
    for t in client.list_tables(f"{PROJECT_ID}.{DATASET_ID}"):
        print(f"  {t.table_id}")
    print(f"\nUpdate TABLE_ID in extract_gsc.py to the correct table name above.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(OUTPUT_PATH),
                        help="Output CSV path")
    parser.add_argument("--list-tables", action="store_true",
                        help="List all tables in the dataset and exit")
    args = parser.parse_args()

    print(f"Connecting to BigQuery project '{PROJECT_ID}'...")
    client = bigquery.Client(project=PROJECT_ID)

    if args.list_tables:
        list_tables(client)
        return

    print(f"Querying {TABLE_ID}...")
    try:
        df = client.query(QUERY).to_dataframe()
    except NotFound:
        print(f"\nERROR: Table '{TABLE_ID}' not found.")
        list_tables(client)
        raise SystemExit(1)

    print(f"  {len(df):,} rows fetched")
    print(f"  Date range: {df['data_date'].min()} to {df['data_date'].max()}")
    print(f"  Markets: {df['country_code'].nunique()}")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    size_mb = os.path.getsize(out) / (1024 * 1024)
    print(f"\nSaved {out} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
