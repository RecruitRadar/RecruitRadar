from datetime import datetime
import time
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DEFAULT_ARGS = {
    'owner': 'jd_analysis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False, # True로 설정하면, start_date 이전의 DAG도 실행함
    'retries': 1,
}

def get_airflow_variable_or_default(variable_name, default_value=None):
    try:
        return Variable.get(variable_name)
    except KeyError:
        return default_value

with DAG('glue_crawler_dag', 
         description='Run Glue Crawler',
         schedule_interval=None,
         start_date=datetime(2023, 8, 15),
         default_args=DEFAULT_ARGS) as dag:
    
    @task()
    def start_crawler(crawler_name:str) -> str:
        access_key = get_airflow_variable_or_default("access_key")
        secret_key = get_airflow_variable_or_default("secret_key")
        region_name = get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        glue_client.start_crawler(Name=crawler_name)
        
        message = f"Start crawler: {crawler_name}"
        print(f"Start crawler: {crawler_name}")
        
        return message

        
    @task()
    def check_crawler_status(crawler_name:str) -> str:
        '''
        avoid READY, RUNNING, STOPPING status
        '''
        
        access_key = get_airflow_variable_or_default("access_key")
        secret_key = get_airflow_variable_or_default("secret_key")
        region_name = get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        response = glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
        
        if response == 'READY':
            raise Exception("Crawler is in READY state. Please check the crawler.")
        else:
            while response in ['RUNNING', 'STOPPING']:
                response = glue_client.get_crawler(
                    Name=crawler_name
                    )['Crawler']['State']
                print(f"Current status: {response}")
                time.sleep(4)
            
            else:
                print(f"Current status: {response}")
                return f"Current status: {response}" 
    
    @task()
    def log_final_message() -> None:
        print("All crawlers have completed successfully!")
    
    
    
    trigger_etl_dag_task = TriggerDagRunOperator(
        task_id='trigger_glue_etl_dag',
        trigger_dag_id="glue_etl_job_dag",  # 여기에 트리거링하려는 DAG의 ID를 지정합니다.
        conf={"message": "Triggered from glue_crawler_dag"},
        execution_date="{{ ds }}", # 여기에 트리거링하려는 DAG의 execution_date를 지정합니다. {ds}
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )
    
    crawler_names = ['de1-1-wanted-json-crawler', 'de1-1-jumpit-json-crawler', 'de1-1-jobplanet-json-crawler', 'de1-1-rallit-json-crawler']
    
    last_tasks = []
    for crawler_name in crawler_names:
        last_task = start_crawler(crawler_name=crawler_name) >> check_crawler_status(crawler_name=crawler_name)
        last_tasks.append(last_task)

    # 4. 마지막 PythonOperator 인스턴스를 DAG의 마지막 작업에 연결
    first_crawler_end_task = log_final_message()
    
    etl_crawler_name = "de1-1-1st-cleaned-data-crawler"
    
    last_tasks >> first_crawler_end_task >> trigger_etl_dag_task >> start_crawler(crawler_name=etl_crawler_name) >> check_crawler_status(crawler_name=etl_crawler_name)