DECLARE dt STRING;

SET dt = FORMAT_DATE('%Y%m%d', DATE('{ds}'));

SELECT COUNT(*) as source_table_exists
FROM `{project_id}.{source_dataset_id}.__TABLES__`
WHERE 1=1 
    AND table_id = FORMAT("events_%s", dt)
HAVING COUNT(*) > 0