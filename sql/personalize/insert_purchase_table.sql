MERGE `{project_id}.{target_dataset_id}.{target_table_id}` AS target
USING (
  SELECT 
    ofp.id AS order_fulfillment_product_id,
    o.user_id AS user_id,
    o.updated_at AS updated_at,
    if(ofp.refunded_line_total != 0, 1, 0) AS is_refunded,
    p.id AS item_id,
    'purchase' AS event_type,
    ( pv.price / 1000 ) * ofp.quantity AS event_value,
    UNIX_SECONDS(o.created_at) AS timestamp,
    DATE(TIMESTAMP(o.created_at), "Asia/Seoul") AS partition_date  -- ✅ KST 기준으로 수정 GA가 이벤트를 주는 기준이 KST 기준으로 timestmap를 잘라서 주기 때문에 
  FROM `{project_id}.{source_dataset_id}.hanpoom_orders` AS o 
  LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_order_fulfillment_orders` AS ofo ON o.id = ofo.order_id
  LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_order_fulfillment_products` AS ofp ON ofo.id = ofp.order_fulfillment_id
  LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_product_variants` AS pv ON ofp.product_variant_id = pv.id 
  LEFT JOIN `{project_id}.{source_dataset_id}.hanpoom_products` AS p ON pv.product_id = p.id
  WHERE DATE(TIMESTAMP(o.created_at), "Asia/Seoul") <= '{ds}' -- ✅ 비교 기준도 KST로 맞춤
  and ofp.id is not null
  and p.id is not null
) AS source
ON target.order_fulfillment_product_id = source.order_fulfillment_product_id
    AND target.user_id = source.user_id
    AND target.item_id = source.item_id
    AND target.partition_date = source.partition_date 

WHEN MATCHED AND (
     target.updated_at IS DISTINCT FROM source.updated_at
) THEN
  UPDATE SET
    updated_at = source.updated_at,
    is_refunded = source.is_refunded,
    event_value = source.event_value

WHEN NOT MATCHED THEN
  INSERT (
    order_fulfillment_product_id,
    user_id,
    updated_at,
    is_refunded,
    item_id,
    event_type,
    event_value,
    timestamp,
    partition_date
  )
  VALUES (
    source.order_fulfillment_product_id,
    source.user_id,
    source.updated_at,
    source.is_refunded,
    source.item_id,
    source.event_type,
    source.event_value,
    source.timestamp,
    source.partition_date
  );