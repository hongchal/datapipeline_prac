MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
    WITH preprocessed_data AS (
        SELECT 
            CAST(user_id AS STRING) AS user_id,
            CAST(item_id AS STRING) AS item_id,
            event_type AS event_type,
            CAST(event_value AS FLOAT64) AS event_value,
            SAFE_CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
            partition_date
        FROM `{project_id}.{source_dataset_id}.{source_table_id_1}`
        WHERE timestamp IS NOT NULL

        UNION ALL

        SELECT 
            CAST(user_id AS STRING) AS user_id,
            CAST(item_id AS STRING) AS item_id,
            event_type AS event_type,
            CAST(event_value AS FLOAT64) AS event_value,
            SAFE_CAST(timestamp as int64) as timestamp, -- ✅ timestamp in product db already in seconds
            partition_date
        FROM `{project_id}.{source_dataset_id}.{source_table_id_2}`
        WHERE timestamp IS NOT NULL

        UNION ALL

        SELECT 
            CAST(user_id AS STRING) AS user_id,
            CAST(item_id AS STRING) AS item_id,
            event_type AS event_type,
            CAST(event_value AS FLOAT64) AS event_value,
            SAFE_CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
            partition_date
        FROM `{project_id}.{source_dataset_id}.{source_table_id_3}`
        WHERE timestamp IS NOT NULL

        UNION ALL

        SELECT 
            CAST(user_id AS STRING) AS user_id,
            CAST(item_id AS STRING) AS item_id,
            event_type AS event_type,
            CAST(event_value AS FLOAT64) AS event_value,
            SAFE_CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
            partition_date
        FROM `{project_id}.{source_dataset_id}.{source_table_id_4}`
        WHERE timestamp IS NOT NULL
    )
    
    select * from preprocessed_data
    where 1=1
        and user_id is not null
        and item_id is not null
        and event_value is not null
        and event_type is not null
        and timestamp is not null
        and partition_date <= '{ds}' -- ✅ KST GA give us data by event date and the event date is based on KST
        order by timestamp desc
) AS source

ON target.user_id = source.user_id
    AND target.item_id = source.item_id
    AND target.event_type = source.event_type
    AND target.event_value = source.event_value
    AND target.timestamp = source.timestamp
    AND target.partition_date = source.partition_date

WHEN NOT MATCHED THEN
  INSERT (
    user_id,
    item_id,
    event_type,
    event_value,
    timestamp,
    partition_date
  )
  VALUES (
    source.user_id,
    source.item_id,
    source.event_type,
    source.event_value,
    source.timestamp,
    source.partition_date
  );