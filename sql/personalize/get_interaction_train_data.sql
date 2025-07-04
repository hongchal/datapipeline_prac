SELECT 
    user_id,
    item_id,
    event_type,
    event_value,
    timestamp
FROM `{project_id}.{dataset_id}.{table_id}`
where partition_date <= '{ds}'
ORDER BY timestamp desc