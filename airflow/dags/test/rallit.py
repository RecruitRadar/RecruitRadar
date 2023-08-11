
import asyncio

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from typing import List, Dict, Any
from datetime import date, timedelta
from dotenv import load_dotenv

import pytz
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from plugins.rallit_class import Scraper



@task()
def fetch_data(job_category: str) -> List[Dict[str, Any]]:
    url = 'https://www.rallit.com/'
    scraper = Scraper(base_url=url, selected_job=job_category)
    
    # Getting scraped data
    scraped_data: List[Dict[str, Any]] = asyncio.run(scraper.get_object_thread(start=1, end=30))
    
    return scraped_data

@task(multiple_outputs=True)
def combine_data(*args) -> Dict[str, List[Dict[str, Any]]]:
    combined_data: List[Dict[str, Any]] = []
    
    for data_list in args:
        combined_data.extend(data_list)
        
    return {"data": combined_data}

@task()
def save_to_json(data_list: List[Dict[str, Any]]) -> str:
    return Scraper.save_to_json(data_list=data_list)


@task
def upload_local_file_to_s3(upload_file:str, s3_hook:S3Hook) -> None:
    logging.info("Start upload!")
    
    s3_bucket_name = Variable.get(key='s3_bucket_name')
    today = date.today()
    file_name = f"rallit/year={today.year}/month={today.month:02}/day={today.day:02}/rallit.json"
    logging.info(f'{file_name}' + " upload to " + f'{s3_bucket_name}')
    
    s3_hook.load_file(
        filename=upload_file,
        key= file_name,
        bucket_name=s3_bucket_name,
        replace=True
    )


default_args = {
    'owner': 'jd_analysis',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_tasks': 7,
}

with DAG(
    'rallit_job_scraper_dag',
    default_args=default_args,
    description='A async user rallit_job_scraper_dag',
    start_date=datetime(2023, 8, 1),
    schedule_interval='@once',
) as dag:

    job_categories = Scraper.job_category
    
    fetch_tasks = [fetch_data(job_category=job_category) for job_category in job_categories]
    
    combined_data = combine_data(*fetch_tasks)
    file_path = save_to_json(data_list=combined_data["data"])
    
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    upload_task = upload_local_file_to_s3(upload_file=file_path, s3_hook=s3_hook)
    
    


    

