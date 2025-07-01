SELECT
  sales.products_id,
  sales.quantity,
  sales.revenue,
  product.purchase_price,
  sales.quantity * product.purchase_price AS purchase_cost,
  ROUND(sales.revenue - (sales.quantity * product.purchase_price),2) AS margin
FROM {{ ref('stg_raw__sales') }} AS sales
JOIN {{ ref('stg_raw__product') }} AS product
  ON sales.products_id = product.products_id