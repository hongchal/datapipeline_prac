from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import boto3
import json

@task
def upload_to_s3(bucket_name, table_id, query, **context):
    ds = context['ds']
    
    # AWS Connection 가져오기
    aws_conn = BaseHook.get_connection('aws_default')
    aws_credentials = json.loads(aws_conn.extra) if aws_conn.extra else {}
    
    # AWS 자격증명 파일에서 읽기
    credentials_path = aws_credentials.get('aws_credentials_path')
    if credentials_path:
        with open(credentials_path, 'r') as f:
            creds = json.load(f)
        aws_access_key_id = creds.get('aws_access_key_id')
        aws_secret_access_key = creds.get('aws_secret_access_key')
        region_name = creds.get('region_name', 'us-east-1')
    else:
        aws_access_key_id = aws_credentials.get('aws_access_key_id')
        aws_secret_access_key = aws_credentials.get('aws_secret_access_key')
        region_name = aws_credentials.get('region_name', 'us-east-1')
    
    # S3 클라이언트 생성
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    
    # BigQuery Hook 생성
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    
    # 데이터 조회 실행 (직접 클라이언트 사용)
    client = bq_hook.get_client()
    query_job = client.query(query)
    df = query_job.to_dataframe()
    
    # CSV 형태로 변환
    csv_data = df.to_csv(index=False)
    
    # S3에 업로드
    object_key = f'personalize/{table_id}_{ds}.csv'
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=csv_data,
        ContentType='text/csv'
    )
    
    return f"s3://{bucket_name}/{object_key}"