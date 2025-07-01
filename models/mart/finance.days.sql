SELECT
  sales.date_date,

  COUNT(DISTINCT sales.orders_id) AS total_transactions,

  ROUND(SUM(CAST(sales.revenue AS FLOAT64)), 2) AS total_revenue,

  ROUND(
    SUM(CAST(sales.revenue AS FLOAT64)) / COUNT(DISTINCT sales.orders_id),
    2
  ) AS average_basket,

  ROUND(
    SUM(
      (
        CAST(sales.revenue AS FLOAT64)
        - (sales.quantity * CAST(product.purchase_price AS FLOAT64))
        + CAST(ship.shipping_fee AS FLOAT64)
        - (CAST(ship.logcost AS FLOAT64) - CAST(ship.ship_cost AS FLOAT64))
      )
    ),
    2
  ) AS operational_margin,

  ROUND(SUM(sales.quantity * CAST(product.purchase_price AS FLOAT64)), 2) AS total_purchase_cost,

  ROUND(SUM(CAST(ship.shipping_fee AS FLOAT64)), 2) AS total_shipping_fees,

  ROUND(SUM(CAST(ship.logcost AS FLOAT64)), 2) AS total_log_costs,

  SUM(sales.quantity) AS total_qty_sold

FROM {{ ref('stg_raw__sales') }} AS sales
JOIN {{ ref('stg_raw__product') }} AS product
  ON sales.products_id = product.products_id
JOIN {{ ref('stg_raw__ship') }} AS ship
  ON sales.orders_id = ship.orders_id

GROUP BY sales.date_date
ORDER BY sales.date_date