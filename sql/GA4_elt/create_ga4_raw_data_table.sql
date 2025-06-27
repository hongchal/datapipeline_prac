DECLARE dt STRING;
DECLARE source_table_exists BOOL;

SET dt = FORMAT_DATE('%Y%m%d', DATE('{ds}'));

SET source_table_exists = (
SELECT COUNT(*) > 0
FROM `{project_id}.{source_dataset_id}.__TABLES__`
WHERE table_id = FORMAT("events_%s", dt)
);

IF source_table_exists THEN
EXECUTE IMMEDIATE FORMAT('''
    CREATE TABLE IF NOT EXISTS `{project_id}.{target_dataset_id}.{target_table_id}`
    PARTITION BY partition_date
    AS
    SELECT *, PARSE_DATE('%%Y%%m%%d', event_date) AS partition_date
    FROM `{project_id}.{source_dataset_id}.events_%s`
    WHERE FALSE
''', dt);
END IF;