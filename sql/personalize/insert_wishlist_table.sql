MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
  with wishlist as (
        select
          we.user_id,
          p.id as item_id,
          we.price as event_value,
          we.event_timestamp as timestamp,
          we.partition_date,
          'like' as event_type
    from `{project_id}.{source_dataset_id}.add_to_wishlist_event` as we 
    left join `{project_id}.{source_dataset_2_id}.hanpoom_products` as p on we.shopify_item_id = p.shopify_id
    WHERE we.partition_date <= '{ds}'
  )
  
  select * from wishlist
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