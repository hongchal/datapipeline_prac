create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
as
WITH preprocessed_data AS (
  SELECT 
    CAST(user_id AS STRING) AS user_id,
    CAST(item_id AS STRING) AS item_id,
    event_type AS event_type,
    CAST(event_value AS FLOAT64) AS event_value,
    CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
    partition_date
  FROM `{project_id}.{source_dataset_id}.{source_table_id_1}`
  WHERE partition_date <= '{ds}'

  UNION ALL

  SELECT 
    CAST(user_id AS STRING) AS user_id,
    CAST(item_id AS STRING) AS item_id,
    event_type AS event_type,
    CAST(event_value AS FLOAT64) AS event_value,
    CAST(timestamp as int64) as timestamp, -- ✅ timestamp in product db already in seconds
    partition_date
  FROM `{project_id}.{source_dataset_id}.{source_table_id_2}`
  WHERE partition_date <= '{ds}'

  UNION ALL

  SELECT 
    CAST(user_id AS STRING) AS user_id,
    CAST(item_id AS STRING) AS item_id,
    event_type AS event_type,
    CAST(event_value AS FLOAT64) AS event_value,
    CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
    partition_date
  FROM `{project_id}.{source_dataset_id}.{source_table_id_3}`
  WHERE partition_date <= '{ds}'

  UNION ALL

  SELECT 
    CAST(user_id AS STRING) AS user_id,
    CAST(item_id AS STRING) AS item_id,
    event_type AS event_type,
    CAST(event_value AS FLOAT64) AS event_value,
    CAST(DIV(timestamp, 1000000) AS INT64) AS timestamp,
    partition_date
  FROM `{project_id}.{source_dataset_id}.{source_table_id_4}`
  WHERE partition_date <= '{ds}'
)
SELECT *
FROM preprocessed_data
WHERE false