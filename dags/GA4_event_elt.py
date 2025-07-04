from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.decorators import task
from util.read_sql_file import read_sql_file
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from util.bigquery_custom_sensor import BigQueryDataSensor

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
    dag_id=f'ga4_event_elt_prod',
    start_date=datetime(2025, 6, 29),
    schedule='15 8 * * *',
    catchup=True,
    tags=['ga4', 'event_elt', 'prod'],
) as dag:

    start = start_task()

    with TaskGroup(group_id='user_pseudo_id_elt') as user_pseudo_id_elt:
        create_user_pseudo_id_table = BigQueryInsertJobOperator(
            task_id='create_user_pseudo_id_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_user_pseudo_id_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='user_pseudo_id'
                    ),
                    "useLegacySql": False,
                }
            },
        )

        insert_user_pseudo_id_table = BigQueryInsertJobOperator(
            task_id='insert_user_pseudo_id_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_user_pseudo_id_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='user_pseudo_id'
                    ),
                    "useLegacySql": False,
                }
            },
        )

        create_user_pseudo_id_table >> insert_user_pseudo_id_table

    with TaskGroup(group_id='view_item_event_elt') as view_item_event_elt:
        check_view_item_raw_data_exists = BigQueryDataSensor(
            task_id='check_view_item_raw_data_exists',
            sql_file='event_table_check.sql',
            sql_path='sql/GA4_elt',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='raw_data',
            event_name='view_item',
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
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
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
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
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='view_item_event'
                    ),  
                    "useLegacySql": False,
                }
            },
        )

        check_view_item_raw_data_exists >> create_view_item_event_table >> insert_view_item_event_table

    with TaskGroup(group_id='add_to_cart_event_elt') as add_to_cart_event_elt:
        check_add_to_cart_raw_data_exists = BigQueryDataSensor(
            task_id='check_add_to_cart_raw_data_exists',
            sql_file='event_table_check.sql',
            sql_path='sql/GA4_elt',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='raw_data',
            event_name='add_to_cart',
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_add_to_cart_event_table = BigQueryInsertJobOperator(
            task_id='create_add_to_cart_event_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_ga4_add_to_cart_event_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='add_to_cart_event'
                    ),
                    "useLegacySql": False,
                }
            },
        )

        insert_add_to_cart_event_table = BigQueryInsertJobOperator(
            task_id='insert_add_to_cart_event_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_add_to_cart_event_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='add_to_cart_event'
                    ),  
                    "useLegacySql": False,
                }
            },
        )

        check_add_to_cart_raw_data_exists >> create_add_to_cart_event_table >> insert_add_to_cart_event_table
        
    with TaskGroup(group_id='add_to_wishlist_event_elt') as add_to_wishlist_event_elt:
        check_add_to_wishlist_raw_data_exists = BigQueryDataSensor(
            task_id='check_add_to_wishlist_raw_data_exists',
            sql_file='event_table_check.sql',
            sql_path='sql/GA4_elt',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='raw_data',
            event_name='add_to_wishlist',
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_add_to_wishlist_event_table = BigQueryInsertJobOperator(
            task_id='create_add_to_wishlist_event_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_ga4_add_to_wishlist_event_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='add_to_wishlist_event'
                    ),
                    "useLegacySql": False,
                }
            },
        )

        insert_add_to_wishlist_event_table = BigQueryInsertJobOperator(
            task_id='insert_add_to_wishlist_event_table',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_add_to_wishlist_event_table.sql', 
                        'sql/GA4_elt',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_table_id='add_to_wishlist_event'
                    ),
                    "useLegacySql": False,
                }
            },
        )

        check_add_to_wishlist_raw_data_exists >> create_add_to_wishlist_event_table >> insert_add_to_wishlist_event_table

    event_elt = [ view_item_event_elt, add_to_cart_event_elt, add_to_wishlist_event_elt]
    
    end = end_task()

    start >> user_pseudo_id_elt >> event_elt >> end