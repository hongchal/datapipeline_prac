from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook
import boto3
import json
from airflow.utils.context import Context

class PersonalizeImportJobSensor(BaseSensorOperator):
    """
    Personalize dataset import job 완료를 체크하는 센서
    """
    template_fields = ['import_job_arn']
    
    def __init__(
        self,
        import_job_arn: str,
        role_arn: str,
        aws_conn_id: str = 'aws_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.import_job_arn = import_job_arn
        self.role_arn = role_arn
        self.aws_conn_id = aws_conn_id
    
    def poke(self, context: Context) -> bool:
        """
        Import job 상태를 체크하여 완료 여부 반환
        """
        self.log.info(f"Checking import job ARN: {self.import_job_arn}")
        
        if not self.import_job_arn or not self.import_job_arn.startswith('arn:'):
            self.log.error(f"Invalid ARN format: {self.import_job_arn}")
            raise ValueError(f"Invalid import job ARN: {self.import_job_arn}")
        # AWS Connection 가져오기
        aws_conn = BaseHook.get_connection(self.aws_conn_id)
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
            RoleArn=self.role_arn,
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
        
        # Import job 상태 확인
        response = personalize_client.describe_dataset_import_job(
            datasetImportJobArn=self.import_job_arn
        )
        
        status = response['datasetImportJob']['status']
        self.log.info(f"Import job status: {status}")
        
        if status == 'ACTIVE':
            self.log.info(f"✅ Import job completed successfully: {self.import_job_arn}")
            return True
        elif status == 'CREATE FAILED':
            failure_reason = response['datasetImportJob'].get('failureReason', 'Unknown error')
            error_msg = f"❌ Import job failed: {self.import_job_arn}. Reason: {failure_reason}"
            self.log.error(error_msg)
            raise Exception(error_msg)
        else:
            self.log.info(f"Import job still in progress: {status}")
            return False
        

class PersonalizeSolutionVersionSensor(BaseSensorOperator):
    """
    Personalize solution version 학습 완료를 체크하는 센서
    """
    template_fields = ['solution_version_arn']
    
    def __init__(
        self,
        solution_version_arn: str,
        role_arn: str,
        aws_conn_id: str = 'aws_default',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.solution_version_arn = solution_version_arn
        self.role_arn = role_arn
        self.aws_conn_id = aws_conn_id
    
    def poke(self, context: Context) -> bool:
        """
        Solution version 상태를 체크하여 완료 여부 반환
        """
        self.log.info(f"Checking solution version ARN: {self.solution_version_arn}")
        
        if not self.solution_version_arn or not self.solution_version_arn.startswith('arn:'):
            self.log.error(f"Invalid ARN format: {self.solution_version_arn}")
            raise ValueError(f"Invalid solution version ARN: {self.solution_version_arn}")
        
        # AWS Connection 가져오기
        aws_conn = BaseHook.get_connection(self.aws_conn_id)
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
            RoleArn=self.role_arn,
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
        
        # Solution version 상태 확인
        response = personalize_client.describe_solution_version(
            solutionVersionArn=self.solution_version_arn
        )
        
        status = response['solutionVersion']['status']
        self.log.info(f"Solution version status: {status}")
        
        if status == 'ACTIVE':
            self.log.info(f"✅ Solution version completed successfully: {self.solution_version_arn}")
            return True
        elif status == 'CREATE FAILED':
            failure_reason = response['solutionVersion'].get('failureReason', 'Unknown error')
            error_msg = f"❌ Solution version training failed: {self.solution_version_arn}. Reason: {failure_reason}"
            self.log.error(error_msg)
            raise Exception(error_msg)
        else:
            # CREATE PENDING, IN PROGRESS 등의 상태
            self.log.info(f"Solution version still in progress: {status}")
            return False