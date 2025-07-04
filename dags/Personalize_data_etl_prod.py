from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.models import Variable
from util.bigquery_custom_sensor import BigQueryDataSensor
from util.read_sql_file import read_sql_file
from util.s3_uploader import upload_to_s3
from util.personalize_sensor import PersonalizeImportJobSensor, PersonalizeSolutionVersionSensor
from util.personalize_import import create_personalize_import_job
from util.personalize_solution import create_personalize_solution_version

@task 
def start_task(**context):
    print("start")
    ds = context['ds']
    print(f"ds: {ds}")
    return ds

@task 
def load_to_s3_check(**context):
    print("load_to_s3_check")
    return True

@task
def end_task():
    print("end")

with DAG(
    dag_id=f'personalize_data_etl_prod',
    start_date=datetime(2025, 6, 29),
    schedule='20 8 * * *',
    catchup=False,
    tags=['personalize', 'data_etl', 'prod'],
) as dag:

    start = start_task()

    create_personalize_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_personalize_dataset',
        dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
        location='US',
        exists_ok=True,
        gcp_conn_id='google_cloud_default',
    )

    with TaskGroup(group_id='purchase_data_extract') as purchase_data_extract:
        check_purchase_data_exists = BigQueryDataSensor(
            task_id='check_purchase_data_exists',
            sql_file='target_table_check_hanpoom_db.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
            source_table_id='hanpoom_orders',
            event_name='purchase',
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_purchase_data = BigQueryInsertJobOperator(
            task_id='create_personalize_purchase_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_purchase_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='purchase_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_purchase_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_purchase_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_purchase_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='purchase_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        check_purchase_data_exists >> create_personalize_purchase_data >> insert_personalize_purchase_data

    with TaskGroup(group_id='view_item_data_extract') as view_item_data_extract:
        check_view_item_data_exists = BigQueryDataSensor(
            task_id='check_view_item_data_exists',
            sql_file='target_table_check_ga4_raw_data.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='view_item_event',
            event_name=None,
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_view_item_data = BigQueryInsertJobOperator(
            task_id='create_personalize_view_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_view_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='view_item_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_view_item_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_view_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_view_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='view_item_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        check_view_item_data_exists >> create_personalize_view_item_data >> insert_personalize_view_item_data

    with TaskGroup(group_id='cart_item_data_extract') as cart_item_data_extract:
        check_cart_item_data_exists = BigQueryDataSensor(
            task_id='check_cart_item_data_exists',
            sql_file='target_table_check_ga4_raw_data.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='add_to_cart_event',
            event_name=None,
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_cart_item_data = BigQueryInsertJobOperator(
            task_id='create_personalize_cart_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_cart_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='cart_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_cart_item_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_cart_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_cart_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='cart_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        check_cart_item_data_exists >> create_personalize_cart_item_data >> insert_personalize_cart_item_data


    with TaskGroup(group_id='wishlist_item_data_extract') as wishlist_item_data_extract:
        check_wishlist_item_data_exists = BigQueryDataSensor(
            task_id='check_wishlist_item_data_exists',
            sql_file='target_table_check_ga4_raw_data.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
            source_table_id='add_to_wishlist_event',
            event_name=None,
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_wishlist_item_data = BigQueryInsertJobOperator(
            task_id='create_personalize_wishlist_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_wishlist_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        source_dataset_2_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='wishlist_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_wishlist_item_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_wishlist_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_wishlist_table.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID'),
                        source_dataset_2_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='wishlist_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        check_wishlist_item_data_exists >> create_personalize_wishlist_item_data >> insert_personalize_wishlist_item_data
    
    data_cleansing = [purchase_data_extract, view_item_data_extract, cart_item_data_extract, wishlist_item_data_extract]

    with TaskGroup(group_id='combine_interaction_data_and_load_to_s3') as combine_interaction_data_and_load_to_s3:
        create_interaction_train_data = BigQueryInsertJobOperator(
            task_id='create_interaction_train_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_interaction_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        source_table_id_1='view_item_data',
                        source_table_id_2='purchase_data',
                        source_table_id_3='cart_data',
                        source_table_id_4='wishlist_data',
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='interaction_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_interaction_train_data = BigQueryInsertJobOperator(
            task_id='insert_interaction_train_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_interaction_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        source_table_id_1='view_item_data',
                        source_table_id_2='purchase_data',
                        source_table_id_3='cart_data',
                        source_table_id_4='wishlist_data',
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='interaction_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        export_to_s3 = upload_to_s3(
            bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
            table_id='interaction/interaction_train_data',
            query=read_sql_file(
                'get_interaction_train_data.sql',
                'sql/personalize',
                ds='{{ ds }}',
                project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                table_id='interaction_train_data'
            )
        )

        create_interaction_train_data >> insert_interaction_train_data >> export_to_s3

    with TaskGroup(group_id='item_data_extract_and_load_to_s3') as item_data_extract_and_load_to_s3:
        check_item_data_exists = BigQueryDataSensor(
            task_id='check_item_data_exists',
            sql_file='target_table_check_hanpoom_db.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
            source_table_id='hanpoom_products',
            event_name=None,
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_item_data = BigQueryInsertJobOperator(
            task_id='create_personalize_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_item_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='item_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_item_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_item_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_item_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='item_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        export_to_s3 = upload_to_s3(
            bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
            table_id='item/item_train_data',
            query=read_sql_file(
                'get_item_train_data.sql',
                'sql/personalize',
                ds='{{ ds }}',
                project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                table_id='item_train_data'
            )
        )

        check_item_data_exists >> create_personalize_item_data >> insert_personalize_item_data >> export_to_s3
    
    with TaskGroup(group_id='user_data_extract_and_load_to_s3') as user_data_extract_and_load_to_s3:
        check_user_data_exists = BigQueryDataSensor(
            task_id='check_user_data_exists',
            sql_file='target_table_check_hanpoom_db.sql',
            sql_path='sql/personalize',
            project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
            source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
            source_table_id='hanpoom_users',
            event_name=None,
            gcp_conn_id='google_cloud_default',
            poke_interval=int(Variable.get('CHECK_INTERVAL')),
            timeout=int(Variable.get('CHECK_TIMEOUT')),
            mode='poke'
        )

        create_personalize_user_data = BigQueryInsertJobOperator(
            task_id='create_personalize_user_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'create_user_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='user_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        insert_personalize_user_data = BigQueryInsertJobOperator(
            task_id='insert_personalize_user_data',
            gcp_conn_id='google_cloud_default',
            configuration={
                "query": {
                    "query": read_sql_file(
                        'insert_user_train_data.sql',
                        'sql/personalize',
                        ds='{{ ds }}',
                        project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                        source_dataset_id=Variable.get('HANPOOM_PROD_DATASET_ID'),
                        target_dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                        target_table_id='user_train_data'
                    ),
                    "useLegacySql": False,
                }
            }
        )

        export_to_s3 = upload_to_s3(
            bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
            table_id='user/user_train_data',
            query=read_sql_file(
                'get_user_train_data.sql',
                'sql/personalize',
                ds='{{ ds }}',
                project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                dataset_id=Variable.get('PERSONALIZE_DATA_PROD_DATASET_ID'),
                table_id='user_train_data'
            )
        )

        check_user_data_exists >> create_personalize_user_data >> insert_personalize_user_data >> export_to_s3

    load_to_s3_check = load_to_s3_check()

    with TaskGroup(group_id='personalize_dataset_import') as personalize_dataset_import:
        @task(task_id='create_personalize_interaction_train_data_import_job')
        def create_interaction_import_job(**context):
            return create_personalize_import_job(
                bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
                dataset_type='interaction',
                dataset_arn=Variable.get('PERSONALIZE_INTERACTION_DATA_PROD_DATASET_ARN'),
                role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
                **context
            )

        @task(task_id='create_personalize_item_train_data_import_job')
        def create_item_import_job(**context):
            return create_personalize_import_job(
                bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
                dataset_type='item',
                dataset_arn=Variable.get('PERSONALIZE_ITEM_DATA_PROD_DATASET_ARN'),
                role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
                **context
            )

        @task(task_id='create_personalize_user_train_data_import_job')
        def create_user_import_job(**context):
            return create_personalize_import_job(
                bucket_name=Variable.get('S3_BUCKET_NAME_PROD'),
                dataset_type='user',
                dataset_arn=Variable.get('PERSONALIZE_USER_DATA_PROD_DATASET_ARN'),
                role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
                **context
            )

        create_personalize_interaction_train_data_import_job = create_interaction_import_job()
        create_personalize_item_train_data_import_job = create_item_import_job()
        create_personalize_user_train_data_import_job = create_user_import_job()

        # Import job 완료 대기 - 전체 경로 사용
        wait_for_interaction_import = PersonalizeImportJobSensor(
            task_id='wait_for_interaction_import_job',
            import_job_arn="{{ task_instance.xcom_pull(task_ids='personalize_dataset_import.create_personalize_interaction_train_data_import_job') }}",
            role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
            poke_interval=30,
            timeout=300,
            mode='poke'
        )

        wait_for_item_import = PersonalizeImportJobSensor(
            task_id='wait_for_item_import_job',
            import_job_arn="{{ task_instance.xcom_pull(task_ids='personalize_dataset_import.create_personalize_item_train_data_import_job') }}",
            role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
            poke_interval=30,
            timeout=3600,
            mode='poke'
        )

        wait_for_user_import = PersonalizeImportJobSensor(
            task_id='wait_for_user_import_job',
            import_job_arn="{{ task_instance.xcom_pull(task_ids='personalize_dataset_import.create_personalize_user_train_data_import_job') }}",
            role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
            poke_interval=30,
            timeout=3600,
            mode='poke'
        )
        
        # 의존성 설정
        create_personalize_interaction_train_data_import_job >> wait_for_interaction_import 
        create_personalize_item_train_data_import_job >> wait_for_item_import 
        create_personalize_user_train_data_import_job >> wait_for_user_import

    # Solution version 생성 및 학습
    with TaskGroup(group_id='retrain_solution') as retrain_solution:
        @task(task_id='create_personalize_solution_version')
        def create_solution_version(**context):
            return create_personalize_solution_version(
                solution_arn=Variable.get('PERSONALIZE_SOLUTION_PROD_ARN'),
                role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
                **context
            )
        
        # Solution version 학습 완료 대기
        wait_for_solution_training = PersonalizeSolutionVersionSensor(
            task_id='wait_for_solution_training',
            solution_version_arn="{{ task_instance.xcom_pull(task_ids='retrain_solution.create_personalize_solution_version') }}",
            role_arn=Variable.get('PERSONALIZE_PROD_ROLE_ARN'),
            poke_interval=300,  # 5분마다 체크
            timeout=7200,      # 2시간 타임아웃
            mode='poke'
        )
        
        solution_version_task = create_solution_version()
        
        # 의존성 설정
        solution_version_task >> wait_for_solution_training

    end = end_task()
   
    start >> create_personalize_dataset >> data_cleansing >> combine_interaction_data_and_load_to_s3 >> load_to_s3_check
    start >> create_personalize_dataset >> item_data_extract_and_load_to_s3 >> load_to_s3_check
    start >> create_personalize_dataset >> user_data_extract_and_load_to_s3 >> load_to_s3_check

    load_to_s3_check >> personalize_dataset_import >> retrain_solution >> end