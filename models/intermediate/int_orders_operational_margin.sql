SELECT
  sales.orders_id,
  sales.date_date,
  sales.revenue,
  sales.quantity,
  ROUND((sales.quantity * product.purchase_price), 2) AS purchase_cost,
  ROUND(sales.revenue - (sales.quantity * product.purchase_price), 2) AS margin,
  ROUND(((sales.revenue - (sales.quantity * product.purchase_price))+CAST(ship.shipping_fee AS FLOAT64)-(CAST(ship.logcost AS FLOAT64) - CAST(ship.ship_cost AS FLOAT64))),2) AS operational_margin
FROM {{ ref('stg_raw__sales') }} AS sales
JOIN {{ ref('stg_raw__product') }} AS product
  ON sales.products_id = product.products_id
JOIN {{ ref('stg_raw__ship') }} AS ship
  ON sales.orders_id = ship.orders_id
ORDER BY sales.date_date DESC