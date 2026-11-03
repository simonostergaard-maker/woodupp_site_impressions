-- ============================================================
--  Switch Australia over to its new Search Console property
--  Run this SQL in BigQuery console, OR add it to the existing
--  scheduled queries that populate combined_url_impressions
--  and combined_site_impressions.
--
--  Australia moved from woodupp.au to woodupp.com.au. The old
--  property's export dataset is searchconsole_woodupp_au; the new
--  property's is searchconsole_searchconsole_woodupp_au (the BigQuery
--  Search Console Data Transfer auto-prefixed it — the double
--  "searchconsole_" is correct, not a typo).
--
--  Both still use country_code = 'au' so this continues the existing
--  "Australia" market line in the dashboard with no code changes.
--  Mirrors the pattern used for adding woodupp.eu
--  (see bigquery_add_eu_market.sql).
-- ============================================================


-- ── 1. URL-level impressions ──────────────────────────────────
-- Add to the scheduled query that updates combined_url_impressions.
-- Safe to re-run: the WHERE clause only inserts dates that don't
-- already have 'au' data (i.e. dates the old property didn't cover).

INSERT INTO `obsidian-375910.woodupp.combined_url_impressions`
SELECT
  data_date,
  site_url,
  url,
  query,
  is_anonymized_query,
  is_anonymized_discover,
  country,
  search_type,
  device,
  is_amp_top_stories,
  is_amp_blue_link,
  is_job_listing,
  is_job_details,
  is_tpf_qa,
  is_tpf_faq,
  is_tpf_howto,
  is_weblite,
  is_action,
  is_events_listing,
  is_events_details,
  is_forums,
  is_search_appearance_android_app,
  is_amp_story,
  is_amp_image_result,
  is_video,
  is_organic_shopping,
  is_review_snippet,
  is_special_announcement,
  is_recipe_feature,
  is_recipe_rich_snippet,
  is_subscribed_content,
  is_page_experience,
  is_practice_problems,
  is_math_solvers,
  is_translated_result,
  is_edu_q_and_a,
  is_product_snippets,
  is_merchant_listings,
  is_learning_videos,
  impressions,
  clicks,
  sum_position,
  'au' AS country_code
FROM `obsidian-375910.searchconsole_searchconsole_woodupp_au.searchdata_url_impression`
WHERE data_date NOT IN (
  SELECT DISTINCT data_date
  FROM `obsidian-375910.woodupp.combined_url_impressions`
  WHERE country_code = 'au'
);


-- ── 2. Site-level impressions ─────────────────────────────────
-- Add to the scheduled query that updates combined_site_impressions.

INSERT INTO `obsidian-375910.woodupp.combined_site_impressions`
SELECT
  data_date,
  site_url,
  query,
  is_anonymized_query,
  country,
  search_type,
  device,
  impressions,
  clicks,
  sum_top_position,
  'au' AS country_code
FROM `obsidian-375910.searchconsole_searchconsole_woodupp_au.searchdata_site_impression`
WHERE data_date NOT IN (
  SELECT DISTINCT data_date
  FROM `obsidian-375910.woodupp.combined_site_impressions`
  WHERE country_code = 'au'
);
