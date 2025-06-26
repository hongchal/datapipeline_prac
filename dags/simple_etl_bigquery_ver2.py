from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
import requests
import pandas as pd
from io import StringIO
from google.cloud import bigquery

default_args = {
    'start_date': datetime(2025, 6, 23),
}

@task
def extract_and_transform_data():
    """Extract data from API and transform to CSV format for BigQuery"""
    url = Variable.get("country_capital_url")
    response = requests.get(url)
    lines = response.text.strip().split("\n")
    records = [line.split(",") for line in lines[1:]]
    
    df = pd.DataFrame(records, columns=["country", "capital"])
    
    # Convert to CSV string for BigQuery load
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    # Store in a temporary location or return for next task
    return csv_data

@task
def upload_to_bigquery(csv_data):
    """Upload CSV data to BigQuery with truncate and load for idempotency"""
    project_id = Variable.get("BIG_QUERY_PROJECT_ID")
    dataset_id = "raw_data"
    table_id = "country_capital"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    hook = BigQueryHook(
        gcp_conn_id='google_cloud_default',
        use_legacy_sql=False
    )
    
    # Create DataFrame from CSV data
    df = pd.read_csv(StringIO(csv_data))
    
    # Use WRITE_TRUNCATE for idempotency (replaces table contents)
    client = hook.get_client(project_id=project_id)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # This truncates and loads
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        schema=[
            bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("capital", "STRING", mode="NULLABLE"),
        ]
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    print(f"✅ {len(df)} rows loaded into {table_ref}")

with DAG(
    dag_id='country_capital_to_bigquery_operator',
    default_args=default_args,
    schedule='0 2 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    # Create dataset if not exists
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='raw_data',
        project_id=Variable.get("BIG_QUERY_PROJECT_ID"),
        location='US',
        exists_ok=True,
        gcp_conn_id='google_cloud_default'
    )
    
    # Create table if not exists
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_table',
        dataset_id='raw_data',
        table_id='country_capital',
        project_id=Variable.get("BIG_QUERY_PROJECT_ID"),
        schema_fields=[
            {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'capital', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        exists_ok=True,
        gcp_conn_id='google_cloud_default'
    )
    
    # ETL tasks
    csv_data = extract_and_transform_data()
    upload_task = upload_to_bigquery(csv_data)
    
    # Set task dependencies
    create_dataset >> create_table >> csv_data >> upload_task