create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
partition by partition_date
as
with add_to_cart as (
  select
    user_id,
    item_id,
    price as event_value,
    event_timestamp as timestamp,
    partition_date,
    'cart' as event_type
  from `{project_id}.{source_dataset_id}.add_to_cart_event` as atc
) 

select * from add_to_cart
where false