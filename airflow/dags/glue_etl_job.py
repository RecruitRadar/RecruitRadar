from datetime import datetime
import time
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

DEFAULT_ARGS = {
    'owner': 'jd_analysis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,  # True로 설정하면, start_date 이전의 DAG도 실행함
    'retries': 1,
}

def get_airflow_variable_or_default(variable_name, default_value=None):
    try:
        return Variable.get(variable_name)
    except KeyError:
        return default_value

with DAG('glue_etl_job_dag', 
         description='Run Glue ETL Job',
         schedule_interval=None,
         start_date=datetime(2023, 8, 15),
         default_args=DEFAULT_ARGS) as dag:

    @task()
    def start_etl_job(job_name: str) -> str:
        access_key = get_airflow_variable_or_default("access_key")
        secret_key = get_airflow_variable_or_default("secret_key")
        region_name = get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        
        response = glue_client.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        return job_run_id
    
    @task()
    def check_etl_job_status(job_name: str, job_run_id: str) -> str:
        access_key = get_airflow_variable_or_default("access_key")
        secret_key = get_airflow_variable_or_default("secret_key")
        region_name = get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        job_run_state = response['JobRun']['JobRunState']

        while job_run_state in ['RUNNING', 'STOPPING']:
            print(f"Current status: {job_run_state}")
            time.sleep(4)
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_run_state = response['JobRun']['JobRunState']
            
            if job_run_state == 'FAILED':
                raise Exception(f"ETL Job failed with status: {job_run_state}")
            
            elif job_run_state == 'SUCCEEDED':
                break
            
        message = f"ETL Job finished with status: {job_run_state}"
        print(message)

        return message

    etl_job_name = '1st_preprocessing'  

    job_run_id = start_etl_job(job_name=etl_job_name)
    check_etl_job_status(job_name=etl_job_name, job_run_id=job_run_id)