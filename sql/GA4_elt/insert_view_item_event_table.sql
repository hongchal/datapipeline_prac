DELETE FROM `{project_id}.{target_dataset_id}.{target_table_id}`
WHERE partition_date = '{ds}';

INSERT INTO `{project_id}.{target_dataset_id}.{target_table_id}`
WITH view_item_data AS (
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
        (SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'item_id_wms') AS item_id,
        item_id as shopify_item_id,
        item_name, 
        item_category,
        (SELECT value.string_value FROM UNNEST(item.item_params) WHERE key = 'item_type') AS shipping_type,
        (SELECT value.string_value FROM UNNEST(item.item_params) WHERE key = 'item_list_type') AS item_list_type,
        COALESCE(
            CAST((SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'regular_price') AS FLOAT64),
            (SELECT value.float_value FROM UNNEST(item.item_params) WHERE key = 'regular_price'),
            (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'regular_price') 
        ) AS regular_price,
        price,
        COALESCE(
            CAST((SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') AS FLOAT64),
            (SELECT value.float_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating'),
            (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') 
        ) AS avg_rating,
        PARSE_DATE('%Y%m%d', event_date) AS partition_date
    FROM `{project_id}.{source_dataset_id}.raw_data`,
    UNNEST(items) AS item
    WHERE event_name = 'view_item'
    AND event_date = FORMAT_DATE('%Y%m%d', DATE('{ds}'))
)
SELECT * FROM view_item_data;