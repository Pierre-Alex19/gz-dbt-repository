SELECT
  FORMAT_DATE('%Y-%m', COALESCE(f.date_date, c.date_date)) AS datemonth,

  -- Métriques publicitaires
  ROUND(SUM(COALESCE(f.operational_margin, 0) - COALESCE(c.total_ads_cost, 0)), 2) AS ads_margin,
  ROUND(
    AVG(
      COALESCE(f.total_revenue, 0) / NULLIF(COALESCE(f.total_qty_sold, 0), 0)
    ),
    2
  ) AS average_basket,
  ROUND(SUM(COALESCE(f.operational_margin, 0)), 2) AS operational_margin,
  ROUND(SUM(COALESCE(c.total_ads_cost, 0)), 2) AS ads_cost,
  SUM(COALESCE(c.total_impressions, 0)) AS ads_impression,
  SUM(COALESCE(c.total_clicks, 0)) AS ads_clicks,

  -- Métriques financières
  SUM(COALESCE(f.total_qty_sold, 0)) AS quantity,
  ROUND(SUM(COALESCE(f.total_revenue, 0)), 2) AS revenue,
  ROUND(SUM(COALESCE(f.total_purchase_cost, 0)), 2) AS purchase_cost,
  ROUND(SUM(COALESCE(f.total_revenue, 0) - COALESCE(f.total_purchase_cost, 0)), 2) AS margin,
  ROUND(SUM(COALESCE(f.total_shipping_fees, 0)), 2) AS shipping_fee,
  ROUND(SUM(COALESCE(f.total_log_costs, 0)), 2) AS log_cost

FROM {{ ref('finance_days') }} f
FULL OUTER JOIN {{ ref('int_campaigns_day') }} c
  ON f.date_date = c.date_date

GROUP BY datemonth
ORDER BY datemonth DESC