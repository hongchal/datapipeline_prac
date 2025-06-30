from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import task
import os
from util.read_sql_file import read_sql_file
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from util.bigquery_custom_sensor import BigQueryDataSensor

ENV = os.getenv('ENV', 'dev')


@task 
def start_task(**context):
    print("start")
    ds = context['ds']
    print(f"ds: {ds}")
    return ds
    
@task
def end_task():
    print("end")

with DAG(
    dag_id=f'ga4_event_elt_{ENV}',
    start_date=datetime(2024, 12, 31) if ENV == 'prod' else datetime(2025, 6, 5),
    schedule='15 8 * * *' if ENV == 'prod' else '15 2 * * *',
    catchup=True,
    tags=['ga4', 'event_elt', ENV],
) as dag:

    start = start_task()

    with TaskGroup(group_id='view_item_event_elt') as view_item_event_elt:
        check_view_item_raw_data_exists = BigQueryDataSensor(
            task_id='check_view_item_raw_data_exists',
            sql_file='event_table_check.sql',
            sql_path='sql/GA4_elt',
            event_name='view_item',
            gcp_conn_id='google_cloud_default',
            poke_interval=60,
            timeout=300,
            mode='poke'
        )

        create_view_item_event_table = BigQueryInsertJobOperator(
            task_id='create_ga4_view_item_event_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_ga4_view_item_event_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
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
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                        target_table_id='view_item_event'
                    ),  
                    "useLegacySql": False,
                }
            },
        )

        check_view_item_raw_data_exists >> create_view_item_event_table >> insert_view_item_event_table
        
    end = end_task()

    start >> view_item_event_elt >> end
