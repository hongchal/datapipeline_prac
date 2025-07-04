from airflow.hooks.base import BaseHook
import boto3
import json
from datetime import datetime

def create_personalize_import_job(bucket_name, dataset_type, dataset_arn, role_arn, **context):
    """
    Personalize dataset import job 생성
    
    Args:
        dataset_type: 데이터셋 타입
        dataset_arn: 데이터셋 ARN
        s3_path: S3 데이터 경로
        role_arn: IAM 역할 ARN
    """
    ds = context['ds']
    s3_path = f's3://{bucket_name}/personalize/{dataset_type}/{dataset_type}_train_data_{ds}.csv'
    print(f"s3_path: {s3_path}")
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
    
    # Import job 이름 생성
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    import_job_name = f"{dataset_type}-import-job-{timestamp}"
    
    # STS 클라이언트 생성 (Cross-account role assume)
    sts_client = boto3.client(
        'sts',
        region_name=region_name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    
    # Role assume
    assumed_role = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='crossAccountPersonalize'
    )
    
    # Assumed role 자격증명 추출
    assumed_credentials = assumed_role['Credentials']
    
    # Personalize 클라이언트 생성 (assumed role 사용)
    personalize_client = boto3.client(
        'personalize',
        region_name=region_name,
        aws_access_key_id=assumed_credentials['AccessKeyId'],
        aws_secret_access_key=assumed_credentials['SecretAccessKey'],
        aws_session_token=assumed_credentials['SessionToken']
    )
    
    # Dataset import job 생성
    response = personalize_client.create_dataset_import_job(
        jobName=import_job_name,
        datasetArn=dataset_arn,
        dataSource={
            'dataLocation': s3_path
        },
        roleArn=role_arn
    )
    
    dataset_import_job_arn = response['datasetImportJobArn']
    
    print(f"✅ Personalize dataset import job created: {dataset_import_job_arn}")
    return dataset_import_job_arn