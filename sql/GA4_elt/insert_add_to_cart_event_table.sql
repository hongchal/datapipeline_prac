MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
    WITH add_to_cart_data AS (
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
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "item_add_origin") AS item_add_origin,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "currency") AS currency,
            COALESCE(
                CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'value') AS FLOAT64),
                (SELECT value.float_value FROM UNNEST(event_params) WHERE key = 'value') ,
                (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value') 
                ) AS value,
            (SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'item_id_wms') AS item_id,
            item_id as shopify_item_id,
            item_name, 
            item_variant,
            item_category,
            (SELECT value.string_value FROM UNNEST(item.item_params) WHERE key = 'item_type') AS shipping_type,
            (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'regular_price') AS regular_price,
            price,
            COALESCE(
                CAST((SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') AS FLOAT64),
                (SELECT value.float_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') ,
                (SELECT value.double_value FROM UNNEST(item.item_params) WHERE key = 'avg_rating') 
            ) AS avg_rating,
            (SELECT value.int_value FROM UNNEST(item.item_params) WHERE key = 'review_count') AS review_count,
            quantity,
            PARSE_DATE('%Y%m%d', event_date) AS partition_date
        FROM `{project_id}.{source_dataset_id}.raw_data` as raw_data
        left join `{project_id}.{source_dataset_id}.user_pseudo_id` as upi on raw_data.user_pseudo_id = upi.user_pseudo_id,
        UNNEST(items) AS item
        WHERE event_name = 'add_to_cart'
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
        item_add_origin,
        currency,
        item_id,
        shopify_item_id,
        item_name,
        item_variant,
        item_category,
        shipping_type,
        regular_price,
        price,
        avg_rating,
        review_count,
        quantity,
        partition_date 
    FROM add_to_cart_data
    WHERE 1=1 
    AND user_id is not null
    AND item_id is not null
    AND event_timestamp is not null
    AND price is not null
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
        user_id, event_timestamp, event_name, item_id, partition_date,
        page_location, ga_session_id, ga_session_number, user_pseudo_id
        ORDER BY event_timestamp DESC
    ) = 1
) AS source
ON target.user_id = source.user_id
    AND target.event_timestamp = source.event_timestamp
    AND target.event_name = source.event_name
    AND target.item_id = source.item_id
    AND target.partition_date = source.partition_date
    AND target.page_location = source.page_location
    AND target.ga_session_id = source.ga_session_id
    AND target.ga_session_number = source.ga_session_number
    AND target.user_pseudo_id = source.user_pseudo_id
WHEN MATCHED AND (
    target.item_add_origin IS DISTINCT FROM source.item_add_origin
) THEN
  UPDATE SET
    target.regular_price = source.regular_price,
    target.price = source.price,
    target.avg_rating = source.avg_rating,
    target.shipping_type = source.shipping_type,
    target.item_variant = source.item_variant,
    target.item_category = source.item_category,
    target.review_count = source.review_count,
    target.quantity = source.quantity,
    target.currency = source.currency,
    target.item_add_origin = source.item_add_origin,
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
    item_add_origin,
    currency,
    item_id,
    shopify_item_id,
    item_name,
    item_variant,
    item_category,
    shipping_type,
    regular_price,
    price,
    avg_rating,
    review_count,
    quantity,
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
    source.item_add_origin,
    source.currency,
    source.item_id,
    source.shopify_item_id,
    source.item_name,
    source.item_variant,
    source.item_category,
    source.shipping_type,
    source.regular_price,
    source.price,
    source.avg_rating,
    source.review_count,
    source.quantity,
    source.partition_date
  );