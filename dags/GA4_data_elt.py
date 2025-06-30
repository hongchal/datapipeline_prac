from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from datetime import datetime
from airflow.decorators import task
from airflow.models import Variable
from util.read_sql_file import read_sql_file
import os 

ENV = os.getenv('ENV', 'dev')

default_args = {
    'start_date': datetime(2024, 12, 31) if ENV == 'prod' else datetime(2025, 6, 5),
}

@task 
def start_task(**context):
    print("start")
    print(f"ds: {context['ds']}")
    
@task
def end_task():
    print("end")

with DAG(   
    dag_id= f'ga4_raw_data_elt_v1.0_{ENV}',
    default_args=default_args,
    schedule= '10 8 * * *' if ENV == 'prod' else '10 2 * * *',  
    catchup=True,
    tags=['ga4', 'raw_data_elt', 'prod'] if ENV == 'prod' else ['ga4', 'raw_data_elt', 'dev'],
) as dag:
    start = start_task()
    end = end_task()

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_ga4_events_dataset',
        dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
        location='US',
        exists_ok=True,
        gcp_conn_id='google_cloud_default',
    )

    create_table = BigQueryInsertJobOperator(
        task_id='create_partitioned_table_if_not_exists',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": read_sql_file(
                    'create_ga4_raw_data_table.sql',
                    'sql/GA4_elt',
                    ds='{{ ds }}',
                    project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                    source_dataset_id=Variable.get('GA4_PROD_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_DATASET_ID'),
                    target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_table_id='raw_data'
                ),
                "useLegacySql": False,
            }
        },
    )

    run_partition_insert = BigQueryInsertJobOperator(
        task_id='run_partition_insert_if_table_exists',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": read_sql_file(
                    'insert_ga4_raw_data.sql',
                    'sql/GA4_elt',
                    ds='{{ ds }}',      
                    project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                    source_dataset_id=Variable.get('GA4_PROD_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_DATASET_ID'),
                    target_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                    target_table_id='raw_data'
                ),
                "useLegacySql": False,
            }
        },
    )

    start >> create_dataset >> create_table >> run_partition_insert >> end
