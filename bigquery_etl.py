from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import os
import requests

load_dotenv()

def bigquery_connection():
    """BigQuery 클라이언트 연결"""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
    
    # 서비스 계정 키 파일 경로 설정 (필요한 경우)
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    client = bigquery.Client(project=project_id)
    return client

def extract(url):
    """URL에서 데이터 추출"""
    response = requests.get(url)
    return response.text

def transform(text):
    """CSV 텍스트를 레코드 리스트로 변환 (헤더 제외)"""
    lines = text.strip().split("\n")
    records = []
    for line in lines[1:]:  # 헤더 제외
        if line.strip():  # 빈 줄 제외
            parts = line.split(",", 1)  # 최대 1번만 분할 (국가명에 쉼표가 있을 수 있음)
            if len(parts) == 2:
                country, capital = parts
                records.append({"country": country.strip(), "capital": capital.strip()})
    return records

def load_to_bigquery(client, records, dataset_id, table_id):
    """BigQuery 테이블에 데이터 로드 (Full Refresh)"""
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # 스키마 정의
    schema = [
        bigquery.SchemaField("country", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("capital", "STRING", mode="REQUIRED"),
    ]
    
    # 테이블 생성 또는 재생성
    table = bigquery.Table(table_ref, schema=schema)
    
    # 기존 테이블이 있으면 삭제하고 새로 생성 (Full Refresh)
    try:
        client.delete_table(table_ref)
        print(f"기존 테이블 {dataset_id}.{table_id} 삭제됨")
    except Exception as e:
        print(f"테이블이 존재하지 않아 삭제 건너뜀: {e}")
    
    # 새 테이블 생성
    table = client.create_table(table)
    print(f"테이블 {dataset_id}.{table_id} 생성됨")
    
    # 데이터 삽입
    if records:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        # 데이터를 DataFrame으로 변환
        df = pd.DataFrame(records)
        
        # BigQuery에 로드
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # 작업 완료 대기
        
        print(f"{len(records)}개 레코드가 {dataset_id}.{table_id}에 로드됨")

def check_table_stats(client, dataset_id, table_id):
    """테이블 통계 확인"""
    query = f"""
    SELECT *
    FROM `{client.project}.{dataset_id}.{table_id}`
    ORDER BY country
    LIMIT 5
    """
    
    # 전체 레코드 수 조회
    count_query = f"""
    SELECT COUNT(*) as total_count
    FROM `{client.project}.{dataset_id}.{table_id}`
    """
    
    count_result = client.query(count_query).result()
    total_count = list(count_result)[0].total_count
    
    # 상위 5개 레코드 조회
    query_result = client.query(query).result()
    
    print(f"총 레코드 수: {total_count}")
    print("상위 5개 레코드:")
    for row in query_result:
        print(f"  {row.country} - {row.capital}")
    
    return total_count

def get_table_schema_info(client, dataset_id, table_id):
    """테이블 스키마 정보 조회"""
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    print(f"테이블: {table.project}.{table.dataset_id}.{table.table_id}")
    print(f"생성일: {table.created}")
    print(f"총 행 수: {table.num_rows:,}")
    print(f"테이블 크기: {table.num_bytes:,} bytes")
    print("\n컬럼 정보:")
    
    columns_info = []
    for field in table.schema:
        column_info = {
            'column_name': field.name,
            'data_type': field.field_type,
            'mode': field.mode,
            'description': field.description or ''
        }
        columns_info.append(column_info)
        print(f"  - {field.name}: {field.field_type} ({field.mode})")
    
    return columns_info

def query_table_to_dataframe(client, dataset_id, table_id, limit=10, order_by=None):
    """테이블 데이터를 조회하여 DataFrame으로 변환"""
    # 테이블 스키마 정보 먼저 확인
    schema_info = get_table_schema_info(client, dataset_id, table_id)
    
    # 컬럼명 리스트 생성
    column_names = [col['column_name'] for col in schema_info]
    print(f"\n컬럼 목록: {column_names}")
    
    # 쿼리 작성
    order_clause = f"ORDER BY {order_by}" if order_by else ""
    query = f"""
    SELECT *
    FROM `{client.project}.{dataset_id}.{table_id}`
    {order_clause}
    LIMIT {limit}
    """
    
    try:
        print(f"\n실행 쿼리:\n{query}")
        query_result = client.query(query).result()
        
        # DataFrame으로 변환
        df = query_result.to_dataframe()
        
        print(f"\nDataFrame 정보:")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\nDataFrame 내용:")
        print(df)
        
        return df, column_names, schema_info
        
    except Exception as e:
        print(f"테이블 조회 실패: {e}")
        return None, column_names, schema_info

def nps_data_query(client, dataset_id, table_id):
    """NPS 데이터 조회 (Snowflake의 NPS 테이블과 유사한 구조)"""
    return query_table_to_dataframe(client, dataset_id, table_id, limit=10, order_by="id")

def query_ga4_events(client, dataset_id="ga4_events_partitioned", table_id="events", limit=5):
    """GA4 이벤트 테이블 조회"""
    return query_table_to_dataframe(client, dataset_id, table_id, limit=limit, order_by="partition_date DESC")

def country_capital_etl_pipeline(url, dataset_id="raw_data", table_id="country_capital"):
    """국가-수도 ETL 파이프라인"""
    try:
        # BigQuery 클라이언트 초기화
        client = bigquery_connection()
        
        # ETL 단계 실행
        print("1. 데이터 추출 중...")
        data = extract(url)
        
        print("2. 데이터 변환 중...")
        records = transform(data)
        print(f"변환된 레코드 수: {len(records)}")
        
        print("3. BigQuery에 데이터 로드 중...")
        load_to_bigquery(client, records, dataset_id, table_id)
        
        print("4. 테이블 통계 확인 중...")
        total_count = check_table_stats(client, dataset_id, table_id)
        
        print(f"ETL 파이프라인 완료! 총 {total_count}개 레코드 처리됨")
        
    except Exception as e:
        print(f"ETL 파이프라인 실행 중 오류 발생: {e}")
        raise e

def create_nps_sample_table(client, dataset_id="raw_data", table_id="nps"):
    """샘플 NPS 테이블 생성 (Snowflake 예제와 유사)"""
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # 스키마 정의
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("created", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("score", "INTEGER", mode="REQUIRED"),
    ]
    
    # 테이블 생성
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        client.delete_table(table_ref)
        print(f"기존 NPS 테이블 삭제됨")
    except:
        pass
    
    table = client.create_table(table)
    print(f"NPS 테이블 생성됨: {dataset_id}.{table_id}")
    
    # 샘플 데이터 삽입
    sample_data = [
        {"id": 1, "created": "2019-01-01 22:04:21", "score": 1},
        {"id": 2, "created": "2019-01-02 03:47:26", "score": 9},
        {"id": 3, "created": "2019-01-03 19:31:41", "score": 10},
        {"id": 4, "created": "2019-01-04 05:02:11", "score": 10},
        {"id": 5, "created": "2019-01-04 18:02:53", "score": 1},
        {"id": 6, "created": "2019-01-04 23:06:18", "score": 2},
        {"id": 7, "created": "2019-01-05 13:45:47", "score": 0},
        {"id": 8, "created": "2019-01-05 09:26:13", "score": 2},
        {"id": 9, "created": "2019-01-05 22:36:38", "score": 6},
        {"id": 10, "created": "2019-01-06 13:22:47", "score": 10},
    ]
    
    df = pd.DataFrame(sample_data)
    df['created'] = pd.to_datetime(df['created'])
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    print(f"샘플 NPS 데이터 {len(sample_data)}개 레코드 삽입됨")

if __name__ == "__main__":
    # 환경 변수 확인
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
    if not project_id:
        print("GOOGLE_CLOUD_PROJECT_ID 환경 변수를 설정해주세요.")
        exit(1)
    
    # 국가-수도 ETL 파이프라인 실행
    url = "https://s3-geospatial.s3.us-west-2.amazonaws.com/country_capital.csv"
    
    print("=== BigQuery ETL 파이프라인 시작 ===")
    country_capital_etl_pipeline(url)
    
    print("\n=== NPS 샘플 테이블 생성 ===")
    client = bigquery_connection()
    create_nps_sample_table(client)
    df, columns, schema = nps_data_query(client, "raw_data", "nps")
    
    print("\n=== GA4 이벤트 테이블 조회 예제 ===")
    # GA4 테이블이 존재하는 경우 조회
    try:
        ga4_df, ga4_columns, ga4_schema = query_ga4_events(client)
        print("GA4 테이블 조회 완료")
    except Exception as e:
        print(f"GA4 테이블 조회 실패 (테이블이 없을 수 있음): {e}")
    
    print("\n=== 사용 예제 ===")
    print("# 테이블 정보와 데이터를 DataFrame으로 가져오기:")
    print("client = bigquery_connection()")
    print("df, columns, schema = query_table_to_dataframe(client, 'dataset_name', 'table_name', limit=10)")
    print("print('컬럼:', columns)")
    print("print('데이터:', df.head())")