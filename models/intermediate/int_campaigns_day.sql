SELECT
  date_date,
  COUNT(DISTINCT campaign_key) AS nb_campaigns,
  SUM(ads_cost) AS total_ads_cost,
  SUM(impression) AS total_impressions,
  SUM(click) AS total_clicks
FROM {{ ref('int_campaigns') }}
GROUP BY date_date
ORDER BY date_date DESC