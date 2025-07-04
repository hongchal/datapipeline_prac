from airflow.hooks.base import BaseHook
import boto3
import json

def create_personalize_solution_version(solution_arn, role_arn, **context):
    """
    Personalize solution version 생성 및 완료 대기
    
    Args:
        solution_arn: Solution ARN
        role_arn: IAM 역할 ARN
    """
    print(f"Creating solution version for: {solution_arn}")
    
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
    
    # Solution version 생성
    response = personalize_client.create_solution_version(
        solutionArn=solution_arn
    )
    
    solution_version_arn = response.get('solutionVersionArn')
    
    if not solution_version_arn:
        error_msg = '❌ Failed to create solution version: ARN is undefined.'
        print(error_msg)
        raise Exception(error_msg)
    
    print(f"✅ Solution version created: {solution_version_arn}")
    
    return solution_version_arn