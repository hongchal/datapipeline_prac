SELECT COUNT(*) as record_count
FROM `{project_id}.{source_dataset_id}.{source_table_id}`
WHERE 1=1 
    AND event_name = '{event_name}'
    AND event_date = FORMAT_DATE('%Y%m%d', DATE('{ds}'))
HAVING COUNT(*) > 0