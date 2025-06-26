from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.models import Variable
from datetime import datetime
from airflow.decorators import task

default_args = {
    'start_date': datetime(2025, 6, 20),
}

@task 
def start_task(**context):
    print("start")
    print(f"ds: {context['ds']}")
    
@task
def end_task():
    print("end")

with DAG(
    dag_id='ga4_raw_data_elt',
    default_args=default_args,
    schedule='0 5 * * *',  # 매일 5시에 실행
    catchup=True,
    tags=['ga4', 'raw_data_elt'],
) as dag:
    start = start_task()
    end = end_task()

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_ga4_events_dataset',
        dataset_id='ga4_events',
        project_id='sixth-topic-349709',
        location='US',
        exists_ok=True,
        gcp_conn_id='google_cloud_default',
    )

    create_table = BigQueryInsertJobOperator(
        task_id='create_partitioned_table_if_not_exists',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": """
                    DECLARE dt STRING;
                    DECLARE source_table_exists BOOL;

                    SET dt = FORMAT_DATE('%Y%m%d', DATE_SUB(DATE('{{ ds }}'), INTERVAL 1 DAY));

                    SET source_table_exists = (
                    SELECT COUNT(*) > 0
                    FROM `sixth-topic-349709.analytics_412203527.__TABLES__`
                    WHERE table_id = FORMAT("events_%s", dt)
                    );

                    IF source_table_exists THEN
                    EXECUTE IMMEDIATE FORMAT('''
                        CREATE TABLE IF NOT EXISTS `sixth-topic-349709.ga4_events.raw_data`
                        PARTITION BY partition_date
                        AS
                        SELECT *, PARSE_DATE('%%Y%%m%%d', event_date) AS partition_date
                        FROM `sixth-topic-349709.analytics_412203527.events_%s`
                        WHERE FALSE
                    ''', dt);
                    END IF;
                """,
                "useLegacySql": False,
            }
        },
    )

    run_partition_insert = BigQueryInsertJobOperator(
        task_id='run_partition_insert_if_table_exists',
        gcp_conn_id='google_cloud_default',
        configuration={
            "query": {
                "query": """
                    DECLARE dt STRING;
                    DECLARE table_exists BOOL;

                    SET dt = FORMAT_DATE('%Y%m%d', DATE_SUB(DATE('{{ ds }}'), INTERVAL 1 DAY));

                    SET table_exists = (
                    SELECT COUNT(*) > 0
                    FROM `sixth-topic-349709.analytics_412203527.__TABLES__`
                    WHERE table_id = FORMAT("events_%s", dt)
                    );

                    IF table_exists THEN
                    EXECUTE IMMEDIATE FORMAT('''
                        DELETE FROM `sixth-topic-349709.ga4_events.raw_data`
                        WHERE partition_date = PARSE_DATE('%%Y%%m%%d', '%s')
                    ''', dt);
                    
                    EXECUTE IMMEDIATE FORMAT('''
                        INSERT INTO `sixth-topic-349709.ga4_events.raw_data`    
                        SELECT *, PARSE_DATE('%%Y%%m%%d', event_date) AS partition_date
                        FROM `sixth-topic-349709.analytics_412203527.events_%s`
                    ''', dt);
                    END IF;     
                """,
                "useLegacySql": False,
            }
        },
    )

    start >> create_dataset >> create_table >> run_partition_insert >> end
