create table if not exists `{project_id}.{target_dataset_id}.{target_table_id}`
partition by partition_date
as
with purchase_data as (
  select 
    ofp.id AS order_fulfillment_product_id,
    o.user_id AS user_id,
    o.updated_at AS updated_at,
    if(ofp.refunded_line_total != 0, 1, 0) AS is_refunded,
    p.id AS item_id,
    'purchase' AS event_type,
    ( pv.price / 1000 ) * ofp.quantity AS event_value,
    UNIX_SECONDS(o.created_at) AS timestamp,
    DATE(o.created_at) AS partition_date 
  from `{project_id}.{source_dataset_id}.hanpoom_orders` as o 
  left join `{project_id}.{source_dataset_id}.hanpoom_order_fulfillment_orders` as ofo on o.id = ofo.order_id
  left join `{project_id}.{source_dataset_id}.hanpoom_order_fulfillment_products` as ofp
  on ofo.id = ofp.order_fulfillment_id
  left join `{project_id}.{source_dataset_id}.hanpoom_product_variants` as pv 
  on ofp.product_variant_id = pv.id 
  left join `{project_id}.{source_dataset_id}.hanpoom_products` as p 
  on pv.product_id = p.id
  where DATE(o.created_at) <= '{ds}'
  and ofp.id is not null
  and p.id is not null
)
select * from purchase_data
where false