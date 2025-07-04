MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
    WITH add_to_wishlist_data AS (
        SELECT 
            event_date,
            event_timestamp,
            DATETIME(TIMESTAMP_MICROS(event_timestamp), "Asia/Seoul") AS event_datetime_kst,
            event_name,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
            if (raw_data.user_id is null, upi.user_id, raw_data.user_id) as user_id,
            raw_data.user_pseudo_id,
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
        FROM `{project_id}.{source_dataset_id}.raw_data` as raw_data
        left join `{project_id}.{source_dataset_id}.user_pseudo_id` as upi on raw_data.user_pseudo_id = upi.user_pseudo_id,
        UNNEST(items) AS item
        WHERE event_name = 'add_to_wishlist'
        AND event_date <= REPLACE('{ds}', '-', '')
    )
    SELECT
        event_date,
        event_timestamp,
        event_datetime_kst,
        event_name,
        page_location,
        user_id,
        user_pseudo_id,
        ga_session_id,
        ga_session_number,
        shopify_item_id,
        item_name,
        item_variant,
        item_category,
        price,
        avg_rating,
        shipping_type,
        regular_price,
        review_count,
        partition_date
    FROM add_to_wishlist_data
    WHERE 1=1 
    AND user_id is not null
    AND shopify_item_id is not null
    AND event_timestamp is not null
    AND price is not null
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
        user_id, event_timestamp, event_name, shopify_item_id, partition_date,
        page_location, ga_session_id, ga_session_number, user_pseudo_id
        ORDER BY event_datetime_kst DESC
    ) = 1
) AS source
ON target.user_id = source.user_id
    AND target.event_timestamp = source.event_timestamp
    AND target.event_name = source.event_name
    AND target.shopify_item_id = source.shopify_item_id
    AND target.partition_date = source.partition_date
    AND target.page_location = source.page_location
    AND target.ga_session_id = source.ga_session_id
    AND target.ga_session_number = source.ga_session_number
    AND target.user_pseudo_id = source.user_pseudo_id
WHEN MATCHED AND (
     target.item_variant IS DISTINCT FROM source.item_variant
) THEN
  UPDATE SET
    target.regular_price = source.regular_price,
    target.price = source.price,
    target.avg_rating = source.avg_rating,
    target.shipping_type = source.shipping_type,
    target.item_variant = source.item_variant,
    target.item_category = source.item_category,
    target.review_count = source.review_count,
    target.item_name = source.item_name

WHEN NOT MATCHED THEN
  INSERT (
    event_date,
    event_timestamp,
    event_datetime_kst,
    event_name,
    page_location,
    user_id,
    user_pseudo_id,
    ga_session_id,
    ga_session_number,
    shopify_item_id,
    item_name,
    item_variant,
    item_category,
    price,
    avg_rating,
    shipping_type,
    regular_price,
    review_count,
    partition_date
  )
  VALUES (
    source.event_date,
    source.event_timestamp,
    source.event_datetime_kst,
    source.event_name,
    source.page_location,
    source.user_id,
    source.user_pseudo_id,
    source.ga_session_id,
    source.ga_session_number,
    source.shopify_item_id,
    source.item_name,
    source.item_variant,
    source.item_category,
    source.price,
    source.avg_rating,
    source.shipping_type,
    source.regular_price,
    source.review_count,
    source.partition_date
  )