MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
    select 
            CAST(id AS STRING) AS user_id,
            unix_seconds(created_at) as created_at
        from `{project_id}.{source_dataset_id}.hanpoom_users`
        where id is not null
        and created_at is not null
    ) AS source
ON target.user_id = source.user_id
   AND target.created_at = source.created_at

WHEN NOT MATCHED THEN
  INSERT (user_id, created_at) VALUES (source.user_id, source.created_at)