import json
import math
import asyncio
import aiohttp
import boto3
import os

from datetime import date, datetime
from typing import Any, Dict, List


class JobPlanetScraper:

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
            
    
    def __init__(self, base_url: str, category_id: int, category_name: str):
        self.base_url = base_url
        self.category_id = category_id
        self.category_name = category_name
        self.job_descriptions = []
        

    async def fetch_data_from_api(self, session, api_url: str) -> Dict[str, Any]:
        """
        API로부터 response를 확인하고 data를 가져옵니다.
        """
        async with session.get(api_url) as response:
            if response.status != 200:
                print(f"Error occurred: {response.status}")
                return {}
            response_text = await response.text(encoding="utf-8")
            data = json.loads(response_text)

            return data


    async def get_job_lists(self, session, base_url: str) -> List[int]:
        """
        해당 카테고리에 맞는 공고 job_id list를 얻어냅니다.
        """
        page_size = 9
        result = await self.fetch_data_from_api(
            session,
            f'{base_url}api/v3/job/postings?order_by=recent&occupation_level2={self.category_id}&page=1&page_size={page_size}'
        )
        total_page_cnt_float = result['data']['total_count'] / page_size
        total_page_cnt = math.ceil(total_page_cnt_float)

        job_id_lists = []
        for page in range(1, total_page_cnt + 1):
            result = await self.fetch_data_from_api(
                session,
                f'{base_url}api/v3/job/postings?order_by=recent&occupation_level2={self.category_id}&page={page}&page_size={page_size}'
            )
            job_id_lists.extend([recruit['id'] for recruit in result['data']['recruits']])
            await asyncio.sleep(1)

        return job_id_lists
    

    async def get_job_descriptions(self, session, base_url :str, job_id: int) -> None:
        """
        카테고리별로 수집한 JD list를 통해 각 JD의 정보를 얻어냅니다.
        """
        result = await self.fetch_data_from_api(
            session,
            f'{base_url}api/v1/job/postings/{job_id}'
        )

        end_at_text = result['data']['end_at']
        date_object = datetime.strptime(end_at_text, '%Y.%m.%d')
        end_at_text = date_object.strftime('%Y-%m-%d')
        
        val = {
            'job_id': str(job_id),
            'platform': 'jobplanet',
            'category': self.category_name,
            'company': result['data']['name'],
            'title': result['data']['title'],
            'preferred': None if result['data']['preferred_skill'] == "" else result['data']['preferred_skill'].replace("\r\n", "\n"),
            'required': result['data']['required_qualification'].replace("\r\n", "\n"),
            'primary_responsibility': result['data']['primary_responsibility'].replace("\r\n", "\n"),
            'url': f'https://jobplanet.co.kr/job/search?posting_ids%5B%5D={job_id}',
            'end_at': end_at_text,
            'skills': result['data']['skills'],
            'location': None if result['data']['location'] == "" else result['data']['location'],
            'welfare': None if result['data']['benefit'] == "" else result['data']['benefit'].replace("\r\n", "\n"),
            'body': None,
            'company_description': None if result['data']['introduction'] == "" else result['data']['introduction'].replace("\r\n", "\n"),
            'coordinate': None,
        }

        self.job_descriptions.append(val)
        await asyncio.sleep(2)


    async def main(self) -> List[Dict[str, Any]]:
        """
        Scraper Class의 main 함수입니다. 잡플래닛 JD 수집을 비동기로 실행합니다.
        """
        async with aiohttp.ClientSession() as session:
            job_id_lists = await self.get_job_lists(session, self.base_url)
            
            tasks = [self.get_job_descriptions(session, self.base_url, job_id) for job_id in job_id_lists]
            await asyncio.gather(*tasks)
        
        return self.job_descriptions


    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        """
        Uploads the specified file to an AWS S3 bucket.
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)

        FILE_NAME = f'{bucket_name}/year={year}/month={month}/day={day}/jobplanet.json'
        
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        s3.upload_file(file_path, bucket_name, FILE_NAME)
        
        path_name = os.path.join(bucket_name, FILE_NAME)
        print(f"End Upload to s3://{path_name}")


    @staticmethod
    def save_json(total_job_descriptions: List[Dict[str, Any]]) -> str:
        """
        잡플래닛의 모든 Job Descriptions를 Json형식으로 저장합니다. (중복 제거 X)
        """
        file_path = "./static/jobplanet.json"
        
        result = {'results': total_job_descriptions}

        with open(file_path, "w", encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)

        return file_path
