MERGE `{project_id}.{target_dataset_id}.{target_table_id}` T
USING (
    WITH view_item_data AS (
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
        FROM `{project_id}.{source_dataset_id}.raw_data` as raw_data
        left join `{project_id}.{source_dataset_id}.user_pseudo_id` as upi on raw_data.user_pseudo_id = upi.user_pseudo_id,
        UNNEST(items) AS item
        WHERE event_name = 'view_item'
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
        item_id,
        shopify_item_id,
        item_name,
        item_category,
        shipping_type,
        item_list_type,
        regular_price,
        price,
        avg_rating,
        partition_date
    FROM view_item_data
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
) S
ON T.user_id = S.user_id
   AND T.event_timestamp = S.event_timestamp
   AND T.event_name = S.event_name
   AND T.item_id = S.item_id
   AND T.partition_date = S.partition_date
   AND T.page_location = S.page_location
   AND T.ga_session_id = S.ga_session_id
   AND T.ga_session_number = S.ga_session_number
   AND T.user_pseudo_id = S.user_pseudo_id
WHEN MATCHED AND (
    T.price IS DISTINCT FROM S.price
) THEN
  UPDATE SET
    T.regular_price = S.regular_price,
    T.price = S.price,
    T.avg_rating = S.avg_rating,
    T.shipping_type = S.shipping_type,
    T.item_list_type = S.item_list_type,
    T.item_name = S.item_name,
    T.item_category = S.item_category

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
    item_id,
    shopify_item_id,
    item_name,
    item_category,
    shipping_type,
    item_list_type,
    regular_price,
    price,
    avg_rating,
    partition_date
  )
  VALUES (
    S.event_date,
    S.event_timestamp,
    S.event_datetime_kst,
    S.event_name,
    S.page_location,
    S.user_id,
    S.user_pseudo_id,
    S.ga_session_id,
    S.ga_session_number,
    S.item_id,
    S.shopify_item_id,
    S.item_name,
    S.item_category,
    S.shipping_type,
    S.item_list_type,
    S.regular_price,
    S.price,
    S.avg_rating,
    S.partition_date
  );