create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
partition by partition_date
as
with view_item as (
  select
    user_id,
    item_id,
    price as event_value,
    event_timestamp as timestamp,
    partition_date as partition_date,
    'view' as event_type
  from `{project_id}.{source_dataset_id}.view_item_event` 
) 

select * from view_item
where false