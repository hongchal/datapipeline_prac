SELECT COUNT(*) as record_count
FROM `{project_id}.{source_dataset_id}.{source_table_id}`
WHERE 1=1 
    AND DATE(created_at) <= '{ds}'
HAVING COUNT(*) > 0