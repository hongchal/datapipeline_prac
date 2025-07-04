create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
as 
WITH preprocessed_data AS (
    SELECT
    CAST(p.id AS STRING) AS ITEM_ID,                    -- string
    CAST(AVG(pv.price) AS FLOAT64) AS PRICE,            -- float
    ANY_VALUE(ic1.name) AS CATEGORY_L1,                 -- string 
    ANY_VALUE(ic2.name) AS CATEGORY_L2,                 -- string 
    IF(
        IF(SUM(s.available_quantity) > 0, true, false)
        AND IF(MAX(p.published_at) IS NOT NULL, true, false),
        'true',                                           -- ✅ string 값
        'false'
    ) AS IS_AVAILABLE                                   -- string
    FROM `{project_id}.{source_dataset_id}.hanpoom_products` AS p
    LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_product_variants` AS pv
    ON p.id = pv.product_id
    LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_internal_categories` AS ic1
    ON ic1.id = p.super_internal_category_id
    LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_internal_categories` AS ic2
    ON ic2.id = p.base_internal_category_id
    LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_product_variant_item_mapper` AS pim
    ON pim.product_id = p.id
    LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_stocks` AS s
    ON pim.item_id = s.item_id
    GROUP BY p.id
)
SELECT *
FROM preprocessed_data
WHERE false