create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
as
with user_data as (
    select 
        cast(id as string) as user_id,
        unix_seconds(created_at) as created_at
    from `{project_id}.{source_dataset_id}.hanpoom_users`
)
select * from user_data
where false