CREATE TABLE IF NOT EXISTS `{project_id}.{target_dataset_id}.{target_table_id}`
PARTITION BY partition_date
AS
WITH add_to_wishlist_data AS (
    SELECT 
        event_date,
        event_timestamp,
        DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS event_datetime_kst,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
        user_id,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id") AS ga_session_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_number") AS ga_session_number,
        item_id as shopify_item_id,
        item_name, 
        item_variant,
        item_category,
        price,
        COALESCE(
        CAST((SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') AS FLOAT64),
        (SELECT value.float_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') ,
        (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') 
        ) AS avg_rating,
        (SELECT value.string_value FROM UNNEST(item.item_params) WHERE key = 'item_type') AS shipping_type,
        (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'regular_price') AS regular_price,
        (SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'review_count') AS review_count,
        PARSE_DATE('%Y%m%d', event_date) AS partition_date
    FROM `{project_id}.{source_dataset_id}.raw_data`,
    UNNEST(items) AS item
    WHERE event_name = 'add_to_wishlist'
    AND event_date <= REPLACE('{ds}', '-', '')
)
SELECT * FROM add_to_wishlist_data
WHERE FALSE