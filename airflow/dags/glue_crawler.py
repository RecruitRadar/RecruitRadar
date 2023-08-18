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
    def start_crawler(crawler_name:str) -> None:
        access_key = get_airflow_variable_or_default("access_key")
        secret_key = get_airflow_variable_or_default("secret_key")
        region_name = get_airflow_variable_or_default("region_name")
        glue_client = boto3.client('glue', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        glue_client.start_crawler(Name=crawler_name)
    
    @task()
    def check_crawler_status(crawler_name:str) -> None:
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
                return None 

    crawler_name = 'de-1-1-dw-crawler'

    start_crawler(crawler_name=crawler_name) >> check_crawler_status(crawler_name=crawler_name)