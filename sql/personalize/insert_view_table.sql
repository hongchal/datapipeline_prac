MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
  with view_item as (
        select
        user_id,
        item_id,
        price as event_value,
        event_timestamp as timestamp,
        PARSE_DATE('%Y%m%d', vi.event_date) AS partition_date,
        'view' as event_type
    from `{project_id}.{source_dataset_id}.view_item_event` as vi
    WHERE vi.partition_date <= '{ds}'
  )
  
  select * from view_item
  where user_id is not null
  and item_id is not null
  and event_value is not null
) AS source
ON target.user_id = source.user_id
    AND target.item_id = source.item_id
    AND target.timestamp = source.timestamp

WHEN MATCHED AND (
     target.partition_date IS DISTINCT FROM source.partition_date
) THEN
  UPDATE SET
    event_value = source.event_value,
    event_type = source.event_type

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