from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import task
import os
from util.read_sql_file import read_sql_file
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

ENV = os.getenv('ENV', 'dev')

@task 
def start_task(**context):
    print("start")
    ds_from_conf = context['dag_run'].conf.get('ds') if context['dag_run'] else None
    print(f"ds: {ds_from_conf}")
    return ds_from_conf
    
@task
def end_task():
    print("end")

with DAG(
    dag_id=f'ga4_event_elt_{ENV}',
    start_date=datetime(2024, 12, 31) if ENV == 'prod' else datetime(2025, 6, 5),
    schedule=None,  # 트리거로만 실행
    catchup=False,  # 트리거 방식에서는 catchup 불필요
    tags=['ga4', 'event_elt', ENV],
) as dag:

    start = start_task()

    create_view_item_event_table = BigQueryInsertJobOperator(
        task_id='create_ga4_view_item_event_table',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": read_sql_file(
                    'create_ga4_view_item_event_table.sql', 
                    'sql/GA4_elt',
                    ds='{{ ti.xcom_pull(task_ids="start_task") }}',  
                    project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                    source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_table_id='view_item_event'
                ),
                "useLegacySql": False,
            }
        },
    )

    insert_view_item_event_table = BigQueryInsertJobOperator(
        task_id='insert_view_item_event_table',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": read_sql_file(
                    'insert_view_item_event_table.sql', 
                    'sql/GA4_elt',
                    ds='{{ ti.xcom_pull(task_ids="start_task") }}',
                    project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                    source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_table_id='view_item_event'
                ),  
                "useLegacySql": False,
            }
        },
    )

    end = end_task()

    view_item_event = create_view_item_event_table >> insert_view_item_event_table
    
    start >> view_item_event >> end
