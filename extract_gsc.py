"""
Extracts Google Search Console data from BigQuery and saves to a CSV file
that preprocess.py can consume.

Requires: google-cloud-bigquery  (pip install google-cloud-bigquery)
Auth:      set GOOGLE_APPLICATION_CREDENTIALS or run `gcloud auth application-default login`

Usage:
    python extract_gsc.py
    python extract_gsc.py --out C:\path\to\output.csv
"""
import argparse
import os
from pathlib import Path
from google.cloud import bigquery

PROJECT_ID  = "obsidian-375910"
TABLE_ID    = "obsidian-375910.woodupp.gsc"   # adjust if table name differs
OUTPUT_PATH = Path(__file__).parent / "data" / "woodupp_url_impressions.csv"


QUERY = f"""
SELECT
    data_date,
    country_code,
    CAST(impressions AS INT64)                  AS impressions,
    CAST(clicks AS INT64)                       AS clicks,
    CAST(is_anonymized_query AS BOOL)           AS is_anonymized_query,
    CAST(is_anonymized_discover AS BOOL)        AS is_anonymized_discover,
    CAST(sum_position AS FLOAT64)               AS sum_position,
    url,
    query,
    country,
    device,
    search_type
FROM `{TABLE_ID}`
ORDER BY data_date, country_code
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(OUTPUT_PATH),
                        help="Output CSV path (default: data/woodupp_url_impressions.csv)")
    args = parser.parse_args()
    out = Path(args.out)

    print(f"Connecting to BigQuery project '{PROJECT_ID}'...")
    client = bigquery.Client(project=PROJECT_ID)

    print(f"Querying {TABLE_ID}...")
    df = client.query(QUERY).to_dataframe()
    print(f"  {len(df):,} rows fetched")
    print(f"  Date range: {df['data_date'].min()} to {df['data_date'].max()}")
    print(f"  Markets: {df['country_code'].nunique()}")

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)

    size_mb = os.path.getsize(out) / (1024 * 1024)
    print(f"\nSaved {out} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
