SELECT  
       TIMESTAMP_MICROS(CAST(event_timestamp AS INT64)) AS event_timestamp,
       user_pseudo_id,
       (SELECT value.int_value FROM UNNEST (event_params) WHERE key = 'ga_session_id') AS ga_session_id,
       event_name,
       geo.country,
       device.category,
       traffic_source.source,
       traffic_source.medium,
       traffic_source.name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _table_suffix BETWEEN '20210101' AND '20211231'
AND event_name IN (
       'session_start',
       'view_item',
       'add_to_cart',
       'begin_checkout',
       'add_shipping_info',
       'add_payment_info',
       'purchase'      
)
ORDER BY event_timestamp
LIMIT 1000




