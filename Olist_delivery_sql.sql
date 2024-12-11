-- Extract and process order details with calculated metrics
WITH
  orders AS (
    SELECT
      o.order_id,
      o.customer_id,
      o.order_status,
      DATE(o.order_purchase_timestamp) AS order_purchase_date, -- Converted to date
      o.order_purchase_timestamp,
      o.order_approved_at,
      o.order_delivered_carrier_date,
      o.order_delivered_customer_date,
      o.order_estimated_delivery_date,
      -- Calculated metrics in hours and days
      CAST(DATETIME_DIFF(o.order_approved_at, o.order_purchase_timestamp, HOUR) AS FLOAT64) AS hours_to_approve,
      CAST(DATETIME_DIFF(o.order_delivered_carrier_date, o.order_approved_at, HOUR) / 24.0 AS FLOAT64) AS days_to_carrier,
      CAST(DATETIME_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, HOUR) / 24.0 AS FLOAT64) AS days_carrier_customers,
      CAST(DATETIME_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, HOUR) / 24.0 AS FLOAT64) AS days_to_deliver,
      CAST(DATETIME_DIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp, HOUR) / 24.0 AS FLOAT64) AS estimated_days,
      CAST(DATETIME_DIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date, HOUR) / 24.0 AS FLOAT64) AS difference_days
    FROM
      `tc-da-1.olist_db.olist_orders_dataset` o
  ),

-- Rank and select the most recent review for each order
  reviews_sort AS (
    SELECT
      r.*,
      RANK() OVER (PARTITION BY r.order_id ORDER BY r.review_creation_date DESC, r.review_answer_timestamp DESC) AS reviews_rank
    FROM
      `tc-da-1.olist_db.olist_order_reviews_dataset` r
  ),

-- Filter to include only the latest reviews
  reviews AS (
    SELECT
      *
    FROM
      reviews_sort
    WHERE
      reviews_rank = 1
  ),

-- Aggregate product-related data by order
  product_data AS (
    SELECT
      order_id,
      SUM(price) AS total_price,
      SUM(freight_value) AS total_freight_value
    FROM
      `tc-da-1.olist_db.olist_order_items_dataset`
    GROUP BY
      order_id
  ),

-- Aggregate seller state information by order
  seller_data AS (
    SELECT
      order_id,
      STRING_AGG(DISTINCT seller_state, ', ') AS seller_state
    FROM
      `tc-da-1.olist_db.olist_order_items_dataset` items
    JOIN
      `tc-da-1.olist_db.olist_sellers_dataset` sell
    ON
      sell.seller_id = items.seller_id
    GROUP BY
      order_id
  ),

-- Aggregate payment details by order
  payment_data AS (
    SELECT
      order_id,
      STRING_AGG(DISTINCT payment_type, ', ') AS payment_type,
      SUM(payment_value) AS total_payment
    FROM
      `tc-da-1.olist_db.olist_order_payments_dataset`
    GROUP BY
      order_id
  )

-- Combine all data for final output
SELECT
  -- Order details
  o.order_id,
  o.order_status,
  o.order_purchase_date,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  ROUND(o.hours_to_approve, 3) AS hours_to_approve,
  ROUND(o.days_to_carrier, 1) AS days_to_carrier,
  ROUND(o.days_carrier_customers, 1) AS days_carrier_customers,
  ROUND(o.days_to_deliver, 1) AS days_to_deliver,
  ROUND(o.estimated_days, 1) AS estimated_days,
  ROUND(o.difference_days, 1) AS difference_days,
  
  -- Customer details
  c.customer_unique_id AS customer_id,
  c.customer_zip_code_prefix AS customer_zip_code,
  c.customer_state,
  
  -- Review details
  r.review_id,
  r.review_score,
  
  -- Seller data
  sd.seller_state,
  
  -- Product data
  pd.total_price,
  pd.total_freight_value,
  
  -- Payment data
  pay.payment_type,
  pay.total_payment
  
FROM
  orders o
JOIN
  `tc-da-1.olist_db.olist_customesr_dataset` c
ON
  c.customer_id = o.customer_id
LEFT JOIN
  reviews r
ON
  o.order_id = r.order_id
LEFT JOIN
  product_data pd
ON
  o.order_id = pd.order_id
LEFT JOIN
  seller_data sd
ON
  o.order_id = sd.order_id
LEFT JOIN
  payment_data pay
ON
  o.order_id = pay.order_id

-- Filter for valid and relevant orders
WHERE
  o.order_delivered_customer_date IS NOT NULL
  AND o.order_approved_at IS NOT NULL
  AND o.order_purchase_timestamp >= '2017-01-01'
  AND o.order_status = 'delivered'
ORDER BY
  o.order_purchase_date;
