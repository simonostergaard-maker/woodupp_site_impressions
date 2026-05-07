-- ============================================================
--  Add woodupp.eu to the BigQuery combined tables
--  Run this SQL in BigQuery console, OR add it to the existing
--  scheduled queries that populate combined_url_impressions
--  and combined_site_impressions.
--
--  The EU dataset (searchconsole_woodupp_eu) is populated
--  automatically by the BigQuery Search Console Data Transfer.
--  These queries mirror the pattern used for all other markets.
-- ============================================================


-- ── 1. URL-level impressions ──────────────────────────────────
-- Add to the scheduled query that updates combined_url_impressions.
-- This is safe to re-run: the WHERE clause prevents duplicates.

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
  'eu' AS country_code
FROM `obsidian-375910.searchconsole_woodupp_eu.searchdata_url_impression`
WHERE data_date NOT IN (
  SELECT DISTINCT data_date
  FROM `obsidian-375910.woodupp.combined_url_impressions`
  WHERE country_code = 'eu'
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
  'eu' AS country_code
FROM `obsidian-375910.searchconsole_woodupp_eu.searchdata_site_impression`
WHERE data_date NOT IN (
  SELECT DISTINCT data_date
  FROM `obsidian-375910.woodupp.combined_site_impressions`
  WHERE country_code = 'eu'
);
