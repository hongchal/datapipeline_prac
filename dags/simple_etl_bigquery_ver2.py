from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 23),
}

with DAG(
    dag_id='transform_country_capital_bq',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    # BQ SQL 정의 (예: 대문자 변환 등 간단한 정제)
    query = f"""
    CREATE OR REPLACE TABLE `{Variable.get("BIG_QUERY_PROJECT_ID")}.clean_data.country_capital_cleaned` AS
    SELECT
        country AS country,
        capital AS capital
    FROM `{Variable.get("BIG_QUERY_PROJECT_ID")}.raw_data.country_capital`
    WHERE country IS NOT NULL
    """

    bq_transform = BigQueryInsertJobOperator(
        task_id='bq_transform_clean_data',
        configuration={
            "query": {
                "query": query,
                "useLegacySql": False,
            }
        },
        location="US",  # 사용 중인 BigQuery location으로 변경
        gcp_conn_id="google_cloud_default",  # Airflow Connection에서 설정한 GCP 연결
    )
