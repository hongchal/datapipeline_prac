from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
import pandas as pd
import requests
from io import StringIO
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': datetime(2025, 6, 23),
}

@task
def extract_and_upload_to_gcs():
    """
    API에서 데이터를 추출하고 CSV로 변환한 뒤 GCS에 업로드.
    반환값: GCS URI
    """
    url = Variable.get("country_capital_url")
    response = requests.get(url)
    lines = response.text.strip().split("\n")
    records = [line.split(",") for line in lines[1:]]
    df = pd.DataFrame(records, columns=["country", "capital"])

    # CSV to buffer
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # GCS 업로드
    bucket_name = Variable.get("GCS_BUCKET")  
    object_name = "country_capital.csv"
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

    gcs_hook.upload(
        bucket_name=bucket_name,
        object_name=object_name,
        data=csv_data,
        mime_type="text/csv"
    )

    return f"gs://{bucket_name}/{object_name}"

def load_csv_to_bigquery(
    gcs_uri: str,
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema_fields: list
):
    return BigQueryInsertJobOperator(
        task_id=f"load_{table_id}_from_gcs",
        configuration={
            "load": {
                "sourceUris": [gcs_uri],
                "destinationTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "schema": {"fields": schema_fields},
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule="all_success",  # Optional
    )

with DAG(
    dag_id='gcs_to_bigquery_etl',
    default_args=default_args,
    schedule='0 2 * * *',
    catchup=False,
    tags=['example'],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    gcs_uri = extract_and_upload_to_gcs()

    load_to_bigquery = load_csv_to_bigquery(
        gcs_uri=gcs_uri,
        project_id=Variable.get("BIG_QUERY_PROJECT_ID"),
        dataset_id="raw_data",
        table_id="country_capital",
        schema_fields=[
            {"name": "country", "type": "STRING", "mode": "REQUIRED"},
            {"name": "capital", "type": "STRING", "mode": "NULLABLE"},
        ]
    )

    start >> gcs_uri >> load_to_bigquery >> end
