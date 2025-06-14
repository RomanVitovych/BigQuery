WITH session_data AS (
  SELECT
    CAST (TIMESTAMP_MICROS(CAST(event_timestamp AS INT64)) AS DATE) AS event_date,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    COUNT (DISTINCT CONCAT ( user_pseudo_id, "_", (
        SELECT value.int_value FROM UNNEST (event_params) WHERE KEY = 'ga_session_id'))) AS user_sessions_count,
    COUNTIF (event_name = 'session_start') AS session_start_count,
    COUNTIF (event_name = 'add_to_cart') AS add_to_cart_count,
    COUNTIF (event_name = 'begin_checkout') AS begin_checkout_count,
    COUNTIF (event_name = 'purchase') AS purchase_count
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    ( SELECT value.int_value FROM UNNEST (event_params) WHERE KEY = 'ga_session_id') IS NOT NULL
  GROUP BY
    event_date,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name )
SELECT
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  CASE
    WHEN session_start_count != 0 
    THEN ROUND ((add_to_cart_count/session_start_count*100), 2)
    ELSE NULL
  END AS visit_to_cart,
  CASE
    WHEN session_start_count != 0 
    THEN ROUND ((begin_checkout_count/session_start_count*100), 2)
    ELSE NULL
  END AS visit_to_checkout,
  CASE
    WHEN session_start_count != 0 
    THEN ROUND ((purchase_count/session_start_count*100), 2)
    ELSE NULL
  END AS visit_to_purchase
FROM
  session_data
ORDER BY event_date;


