import os
import json
import math
import asyncio
import aiohttp
import boto3
from datetime import date, datetime
from dotenv import load_dotenv
from typing import Any, Dict

load_dotenv()


class Scraper:
    def __init__(self, base_url)-> None:
        self.base_url = base_url
        self.category_dict = {
            11605: 'DBA',
            11907: 'iOS',
            11601: 'QA',
            11908: '기술지원',
            11609: '네트워크/보안/운영',
            11904: '백엔드 개발',
            11607: '소프트웨어 개발',
            11906: '안드로이드 개발',
            11611: '웹퍼블리셔',
            11905: '프론트엔드 개발',
            11608: '하드웨어 개발',
            11613: '데이터 분석가',
            11914: '데이터 사이언티스트',
            11913: '데이터 엔지니어',
            11915: '머신러닝 엔지니어'
        }

    
    async def fetch_data_from_api(self, api_url: str) -> Dict[str, Any]:
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url) as response:
                if response.status != 200:
                    print(f"Error occurred: {response.status}")
                    return {}
                response_text = await response.text(encoding="utf-8")
                data = json.loads(response_text)

                return data


    async def get_job_lists(self, base_url: str, job_type_id: int) -> Dict[int, Any]:
        page_size = 9
        result = await self.fetch_data_from_api(
            f'{base_url}api/v3/job/postings?order_by=recent&occupation_level2={job_type_id}&page=1&page_size={page_size}'
        )
        total_page_cnt_float = result['data']['total_count'] / page_size
        total_page_cnt = math.ceil(total_page_cnt_float)

        job_lists_val = []
        for page in range(1, total_page_cnt + 1):
            result = await self.fetch_data_from_api(
                f'{base_url}api/v3/job/postings?order_by=recent&occupation_level2={job_type_id}&page={page}&page_size={page_size}'
            )
            job_lists_val.extend(result['data']['recruits'])
            await asyncio.sleep(2)
        
        job_lists = {}
        job_lists[job_type_id] = job_lists_val

        return job_lists


    async def get_job_descriptions(self, base_url :str, job_type_id: int, value: Dict[int, Any]) -> Any:
        job_descriptions = []
        for i in range(len(value[job_type_id])):
            post_id = value[job_type_id][i]['id']
            result = await self.fetch_data_from_api(
                f'{base_url}api/v1/job/postings/{post_id}'
            )

            end_at_text = result['data']['end_at']
            date_object = datetime.strptime(end_at_text, '%Y.%m.%d')
            end_at_text = date_object.strftime('%Y-%m-%d')
            
            val = {
                'job_id': post_id,
                'platform': 'jobplanet',
                'category': self.category_dict[job_type_id],
                'url': f'https://jobplanet.co.kr/job/search?posting_ids%5B%5D={post_id}',
                'company': result['data']['name'],
                'title': result['data']['title'],
                'primary_responsibility': result['data']['primary_responsibility'].replace("\r\n", "\n"),
                'required': result['data']['required_qualification'].replace("\r\n", "\n"),
                'preferred': result['data']['preferred_skill'].replace("\r\n", "\n"),
                'end_at': end_at_text,
                'skills': result['data']['skills'],
                'location': result['data']['location'],
                'body': '',
                'company_description': result['data']['introduction'].replace("\r\n", "\n"),
                'coordinate': [],
            }

            job_descriptions.append(val)
            await asyncio.sleep(2)

        return job_descriptions


    @staticmethod
    def save_json(job_description: Any) -> str:
        file_path = "./results/jobplanet.json"
        with open(file_path, "w", encoding='utf-8') as json_file:
            json.dump(job_description, json_file, ensure_ascii=False, indent=4)

        return file_path


    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        '''Uploads the specified file to an AWS S3 bucket.'''

        print("Start upload!")
        
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        FILE_NAME = f"jobplanet/year={year}/month={month}/day={day}/jobplanet.json"
        
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        
        s3.upload_file(file_path, bucket_name, FILE_NAME)
        
        path_name = os.path.join(bucket_name, FILE_NAME)
        print(f"End Upload to s3://{path_name}")


    async def main(self) -> Any:
        total_jd_list = []

        tasks = [self.get_job_lists(self.base_url, job_type_id) for job_type_id in self.category_dict.keys()]
        values = await asyncio.gather(*tasks)
        
        tasks = [self.get_job_descriptions(self.base_url, job_type_id, value) for job_type_id, value in zip(self.category_dict.keys(), values)]
        results = await asyncio.gather(*tasks)
        for result in results:
            total_jd_list.extend(result)
        
        return total_jd_list


def main() -> None:
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"

    base_url = 'https://www.jobplanet.co.kr/'
    scraper = Scraper(base_url=base_url)
    loop = asyncio.get_event_loop()
    total_jd_list = loop.run_until_complete(scraper.main())

    file_path = scraper.save_json(total_jd_list)

    Scraper.upload_to_s3(
        file_path = file_path,
        bucket_name=bucket_name,
        access_key=access_key,
        secret_key=secret_key, 
        region_name=region_name
    )


if __name__ == '__main__':
    main()
