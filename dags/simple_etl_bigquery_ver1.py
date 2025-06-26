from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

default_args = {
    'start_date': datetime(2025, 6, 23),
}

@task
def etl():
    # 1. Airflow Variable로부터 서비스 계정 구성
    credentials_dict = {
        "type": "service_account",
        "project_id": Variable.get("BIG_QUERY_PROJECT_ID"),
        "private_key_id": "dummy",  # optional
        "private_key": Variable.get("BIG_QUERY_PRIVATE_KEY").replace("\\n", "\n"),
        "client_email": Variable.get("BIG_QUERY_CLIENT_EMAIL"),
        "client_id": Variable.get("BIG_QUERY_CLIENT_ID"),
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{Variable.get('BIG_QUERY_CLIENT_EMAIL').replace('@', '%40')}",
        "universe_domain": Variable.get("BIG_QUERY_UNIVERSE_DOMAIN", default_var="googleapis.com")
    }

    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    project_id = Variable.get("BIG_QUERY_PROJECT_ID")
    client = bigquery.Client(credentials=credentials, project=project_id)

    # 2. 추출 및 변환
    url = Variable.get("country_capital_url")
    response = requests.get(url)
    lines = response.text.strip().split("\n")
    records = [line.split(",") for line in lines[1:]]

    # 3. BigQuery 업로드
    dataset_id = "raw_data"
    table_id = "country_capital"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        client.create_dataset(dataset)

    schema = [
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("capital", "STRING", mode="NULLABLE"),
    ]

    df = pd.DataFrame(records, columns=["country", "capital"])
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

    job = client.load_table_from_dataframe(df, table_ref)
    job.result()

    print(f"✅ {len(df)} rows loaded into {table_ref}")

with DAG(
    dag_id='country_capital_to_bigquery',
    default_args=default_args,
    schedule = '0 2 * * *',
    catchup=False,
    tags=['example'],
) as dag:

    etl()