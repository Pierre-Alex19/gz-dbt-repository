SELECT
  FORMAT_DATE('%Y-%m', f.date_date) AS datemonth,

  -- Métriques publicitaires
  ROUND(SUM(f.operational_margin - c.total_ads_cost), 2) AS ads_margin,
  ROUND(AVG(f.total_revenue / NULLIF(f.total_qty_sold, 0)), 2) AS average_basket, -- exemple de panier moyen
  ROUND(SUM(f.operational_margin), 2) AS operational_margin,
  ROUND(SUM(c.total_ads_cost), 2) AS ads_cost,
  SUM(c.total_impressions) AS ads_impression,
  SUM(c.total_clicks) AS ads_clicks,

  -- Métriques financières
  SUM(f.total_qty_sold) AS quantity,
  ROUND(SUM(f.total_revenue), 2) AS revenue,
  ROUND(SUM(f.total_purchase_cost), 2) AS purchase_cost,
  ROUND(SUM(f.total_revenue - f.total_purchase_cost), 2) AS margin, -- marge brute mensuelle
  ROUND(SUM(f.total_shipping_fees), 2) AS shipping_fee,
  ROUND(SUM(f.total_log_costs), 2) AS log_cost

FROM {{ ref('finance_days') }} f
LEFT JOIN {{ ref('int_campaigns_day') }} c
  ON f.date_date = c.date_date
GROUP BY datemonth
ORDER BY datemonth DESC