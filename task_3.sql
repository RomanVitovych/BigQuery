SELECT
  CAST(TIMESTAMP_MICROS(CAST(event_timestamp AS INT64)) AS DATE) AS event_date,
  REGEXP_EXTRACT((SELECT value.string_value,  
    FROM UNNEST(event_params) WHERE KEY = 'page_location'), r"https?://[^/]+(/[^?]*)") AS page_path,
  COUNT(DISTINCT CONCAT(user_pseudo_id, '-', (SELECT value.int_value 
    FROM UNNEST(event_params) WHERE KEY = 'ga_session_id'))) AS uniq_ga_session_id_count,  
  COUNT(DISTINCT (SELECT value.int_value 
    FROM UNNEST(event_params) WHERE KEY = 'transaction_id')) AS purchase_count
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
  AND event_name = 'purchase'
GROUP BY
  event_date,
  page_path
ORDER BY
  event_date


