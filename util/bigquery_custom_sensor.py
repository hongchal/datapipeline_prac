from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.context import Context
from airflow.models import Variable
from util.read_sql_file import read_sql_file
import os

ENV = os.getenv('ENV', 'dev')

class BigQueryDataSensor(BaseSensorOperator):
    def __init__(self, sql_file, sql_path, event_name, gcp_conn_id='google_cloud_default', **kwargs):
        super().__init__(**kwargs)
        self.sql_file = sql_file
        self.sql_path = sql_path
        self.event_name = event_name
        self.gcp_conn_id = gcp_conn_id
    
    def poke(self, context: Context):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id, use_legacy_sql=False)
        try:
            sql = read_sql_file(
                self.sql_file,
                self.sql_path,
                ds=context['ds'],
                project_id=Variable.get('BIG_QUERY_PROJECT_ID'),
                source_dataset_id=Variable.get('GA4_PROD_RAW_DATASET_ID') if ENV == 'prod' else Variable.get('GA4_DEV_RAW_DATASET_ID'),
                source_table_id='raw_data',
                event_name=self.event_name
            )
            self.log.info(f"Executing SQL: {sql}")
            result = hook.get_first(sql)
            return result is not None and len(result) > 0
        except Exception as e:
            self.log.info(f"Query failed: {e}")
            return False