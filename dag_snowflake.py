from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
# from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID ="airflow-tutorial-kaoutar"
LANDING_BUCKET = 'landing_bucket_crime'
BUCKUP_BUCKET = 'landing_hasting_sql_practice'

# PROJECT_ID = Variable.get("project")
# LANDING_BUCKET = Variable.get("landing_bucket")
# BUCKUP_BUCKET = Variable.get("buckup_bucket")

default_arguments = {'owner': 'kautar nahli', 'start_date':days_ago(1),
                     'email': ['kautarnahli@hotmail.es'],
                     'email_on_failure': False,
                     'email_on_retry': False,
                     'retries': 2,
                     'retry_delay': timedelta(minutes=2) }

def list_objects(bucket=None):
    hook = GCSHook()
    storage_objects = hook.list(bucket)
    #kwargs["ti"].xcom_push(value=storage_objects)
    return storage_objects

def move_objects(
    source_bucket=None,
    destination_bucket=None,
    prefix=None,
    **kwargs):

    storage_objects = kwargs['ti'].xcom_pull(task_ids='list_files')

    hook = GCSHook()

    for storage_object in storage_objects:
        destination_object = storage_object

        if prefix:
            destination_object = '{}/{}'.format(prefix, storage_object)

        hook.copy(source_bucket, storage_object, destination_bucket)
        hook.delete(source_bucket, storage_object)

with DAG ( 
    'data_load',
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_arguments,
    max_active_runs=1,
    user_defined_macros={'project':PROJECT_ID}
    ) as dag:
        
        list_files = PythonOperator(
            task_id='list_files',
            python_callable=list_objects,
            op_kwargs={'bucket':LANDING_BUCKET},
            provide_context=True
        )
        query = '''
        insert into analytics
        select month, longitude, latitude, crime_type,
            count(*) as number_crimes
        from test
        where month in(
        select max(month) from test
        )
        Group by month,longitude, latitude, crime_type
        Order by number_crimes desc
                
        '''
        query_history_crime = SnowflakeOperator(
            task_id="query_history_crime",
            sql=query,
            snowflake_conn_id="snowflake_conn",
        )

 
    
       

        move_files = PythonOperator(
            task_id='move_files',
            python_callable=move_objects,
            op_kwargs={
                "source_bucket": LANDING_BUCKET,
                "destination_bucket":BUCKUP_BUCKET,
                "prefix": "{{ts_nodash}}",
            },
            provide_context=True

        )

    
# list_files >> load_data_from_bucket >> query_history_crime >> move_files
list_files >> query_history_crime >> move_files
