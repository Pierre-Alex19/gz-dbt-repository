SELECT
  f.date_date AS date,
  -- marge après pub
  ROUND(f.operational_margin - c.total_ads_cost, 2) AS ads_margin,
  f.average_basket,
  f.operational_margin,
  c.total_ads_cost AS ads_cost,
  c.total_impressions AS ads_impression,
  c.total_clicks AS ads_clicks,
  f.total_qty_sold AS quantity,
  f.total_revenue AS revenue,
  f.total_purchase_cost AS purchase_cost,
  f.operational_margin AS margin,
  f.total_shipping_fees AS shipping_fee,
  f.total_log_costs AS log_cost

FROM {{ ref('finance_days') }} f
LEFT JOIN {{ ref('int_campaigns_day') }} c
  ON f.date_date = c.date_date
ORDER BY f.date_date DESC