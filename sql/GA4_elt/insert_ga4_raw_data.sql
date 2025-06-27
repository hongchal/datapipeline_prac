DECLARE dt STRING;
DECLARE table_exists BOOL;

SET dt = FORMAT_DATE('%Y%m%d', DATE('{ds}'));

SET table_exists = (
SELECT COUNT(*) > 0
FROM `{project_id}.{source_dataset_id}.__TABLES__`
WHERE table_id = FORMAT("events_%s", dt)
);

IF table_exists THEN
EXECUTE IMMEDIATE FORMAT('''
    DELETE FROM `{project_id}.{target_dataset_id}.{target_table_id}`
    WHERE partition_date = PARSE_DATE('%%Y%%m%%d', '%s')
''', dt);

EXECUTE IMMEDIATE FORMAT('''
    INSERT INTO `{project_id}.{target_dataset_id}.{target_table_id}`    
    SELECT *, PARSE_DATE('%%Y%%m%%d', event_date) AS partition_date
    FROM `{project_id}.{source_dataset_id}.events_%s`
''', dt);
END IF;