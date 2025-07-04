create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
partition by partition_date
as
with wishlist as (
  select
    aw.user_id,
    p.id as item_id,
    aw.price as event_value,
    aw.event_timestamp as timestamp,
    aw.partition_date as partition_date,
    'like' as event_type
  from `{project_id}.{source_dataset_id}.add_to_wishlist_event` as aw
  left join `{project_id}.{source_dataset_2_id}.hanpoom_products` as p on aw.shopify_item_id = p.shopify_id
) 

select * from wishlist
where false