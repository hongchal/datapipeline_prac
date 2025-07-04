CREATE TABLE IF NOT EXISTS `{project_id}.{target_dataset_id}.{target_table_id}`
AS
WITH user_pseudo_id_data AS (
    SELECT 
        user_pseudo_id,
        MAX(user_id) as user_id
    FROM `{project_id}.{source_dataset_id}.raw_data`
    WHERE user_id is not null
    group by user_pseudo_id
)
SELECT * FROM user_pseudo_id_data
WHERE FALSE