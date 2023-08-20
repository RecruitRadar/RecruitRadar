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
    'catchup': False,
    'retries': 1,
}

def get_airflow_variable(key, default=None):
    return Variable.get(key, default_var=default)

def create_glue_client():
    access_key = get_airflow_variable("access_key")
    secret_key = get_airflow_variable("secret_key")
    region_name = get_airflow_variable("region_name")
    return boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

with DAG('glue_etl_job_dag',
         description='Run Glue ETL Job',
         schedule_interval=None,
         start_date=datetime(2023, 8, 15),
         default_args=DEFAULT_ARGS) as dag:

    @task()
    def start_etl_job(job_name: str) -> str:
        glue_client = create_glue_client()
        response = glue_client.start_job_run(JobName=job_name)
        return response['JobRunId']

    @task()
    def monitor_etl_job(job_name: str, job_run_id: str) -> str:
        glue_client = create_glue_client()
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        job_run_state = response['JobRun']['JobRunState']

        while job_run_state in ['RUNNING', 'STOPPING']:
            print(f"ETL Job status: {job_run_state}")
            time.sleep(4)
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_run_state = response['JobRun']['JobRunState']

            if job_run_state == 'FAILED':
                raise Exception(f"ETL Job failed with status: {job_run_state}")

        return job_run_state


    @task()
    def log_final_message(job_run_state:str) -> None:
        print(f"ETL Job finished with status with {job_run_state}!")
        
        return f"ETL Job finished with status with {job_run_state}!"
        
    etl_job_name = 'de1_1_1st_preprocessing_script'
    job_run_id_extract = start_etl_job(job_name=etl_job_name)
    monitor_task = monitor_etl_job(job_name=etl_job_name, job_run_id=job_run_id_extract)
    log_final_message_task = log_final_message(monitor_task)

    job_run_id_extract >> monitor_task >> log_final_message_task