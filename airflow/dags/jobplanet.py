import json
import asyncio
import io

from datetime import datetime
from typing import Any, Dict, List

from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from plugins.jobplanet_class import Scraper


def get_not_duplicated_jd(total_job_descriptions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    중복되는 job_id를 제거하고, 잡플래닛의 모든 Job Descriptions를 Json형식의 buffer를 전달합니다.
    """
    unique_job_descriptions = []
    seen_job_ids = set()
    for item in total_job_descriptions:
        if item['job_id'] not in seen_job_ids:
            unique_job_descriptions.append(item)
            seen_job_ids.add(item['job_id'])

    result = {'results': unique_job_descriptions}

    return result


def get_file_path(bucket_name, execution_date):
    """
    s3_key가 될 file_path를 생성합니다.
    """
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")
    day = execution_date.strftime("%d")

    return f'{bucket_name}/year={year}/month={month}/day={day}/jobplanet.json' 


@task
def upload_to_s3(bucket_name: str, **kwargs) -> None:
    """
    Uploads the specified file to an AWS S3 bucket.
    """
    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids='jobplanet_scraper') # xcom 고치기 !!!

    execution_date = kwargs['execution_date']
    s3_key = get_file_path(bucket_name, execution_date)

    json_buffer = io.StringIO()
    json.dump(result, json_buffer, ensure_ascii=False, indent=4)
    json_buffer.seek(0)

    json_byte_buffer = io.BytesIO(json_buffer.getvalue().encode('utf-8'))
    json_byte_buffer.seek(0)

    print("Start upload!")

    hook = S3Hook('s3_conn')
    hook.load_file_obj(file_obj=json_byte_buffer, key=s3_key, bucket_name=bucket_name, replace=True)
    
    print("End Upload to s3")


@task
def jobplanet_scraper() -> List[Dict[str, Any]]:
    """
    잡플래닛 Scraper task입니다.
    """
    job_category_dict = {
            11909: 'CTO',
            11605: 'DBA',
            11615: 'ERP',
            11907: 'iOS',
            11601: 'QA',
            11910: 'VR 엔지니어',
            11610: '게임개발',
            11908: '기술지원',
            11609: '네트워크/보안/운영',
            11904: '백엔드 개발',
            11607: '소프트웨어 개발',
            11614: '소프트웨어아키텍트',
            11906: '안드로이드 개발',
            11604: '웹개발',
            11611: '웹퍼블리셔',
            11911: '클라우드 개발',
            11905: '프론트엔드 개발',
            11608: '하드웨어 개발',
            11917: 'BI 엔지니어',
            11613: '데이터 분석가',
            11914: '데이터 사이언티스트',
            11913: '데이터 엔지니어',
            11915: '머신러닝 엔지니어',
            11916: '빅데이터 엔지니어'
        }

    base_url = 'https://www.jobplanet.co.kr/'

    tasks = []
    for category_id, category_name in job_category_dict.items():
        scraper = Scraper(base_url=base_url, category_id=category_id, category_name=category_name)
        task = scraper.main()
        tasks.append(task)

    loop = asyncio.get_event_loop()
    data_list = loop.run_until_complete(asyncio.gather(*tasks))
    
    result = []
    for data in data_list:
        result.extend(data)

    return get_not_duplicated_jd(result)


@dag(
        schedule='0 1 * * *',
        start_date=datetime(2023, 8, 5),
        default_args = {
            'owner': 'airflow',
        },
        catchup=False
)
def jobplanet_job_scraper_dag():
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')

    bucket_name = Variable.get('bucket_name') 
    
    begin \
    >> jobplanet_scraper() \
    >> upload_to_s3(bucket_name=bucket_name) \
    >> end


jobplanet_job_scraper_dag()


if __name__ == "__main__":
    dag_bag = jobplanet_job_scraper_dag.dag
