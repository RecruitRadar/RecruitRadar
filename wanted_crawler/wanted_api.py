from typing import List, Dict, Any
from dotenv import load_dotenv
from datetime import date
import asyncio
import aiohttp
import boto3
import json
import time
import requests
import random
import os



class Scraper:
    """
    A class for scraping job listings from a website and saving them to a JSON file and AWS S3.

    Attributes:
        base_url (str): The base URL of the website to scrape.
        category_id (str): The ID of the job category.
        category_name (str): The name of the job category.
    """
    def __init__(self, base_url: str, category_id: str, category_name: str):

        """Initialize the Scraper class with base URL, category ID, and category name."""

        self.base_url = base_url
        self.category_name = category_name
        self.category_id = category_id
        self.platform = 'wanted'
        self.user_agent_list = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246',
            'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'
            ]
        self.headers = {'User-Agent': random.choice(self.user_agent_list),}
        self.jds = []
        

    async def fetch_data_from_api(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """
        Fetch data from an API endpoint using an aiohttp session.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            url (str): The URL of the API endpoint.

        Returns:
            dict: The JSON response data from the API.
        """
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Error occurred: {response.status}")
                await asyncio.sleep(3)
                return {}
            data =  await response.json()
            return data


    async def fetch_job_url_with_async(self, session: aiohttp.ClientSession) -> List:
        """
        Fetch job URLs asynchronously based on category ID.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the requests.

        Returns:
            list: A list of job IDs.
        """
        print(f'>>> Started collecting job listings for the position of {self.category_name}.')
        job_id_list = []
        url = f'{self.base_url}/api/v4/jobs?country=kr&tag_type_ids={self.category_id}&\
            job_sort=company.response_rate_order&locations=all&years=-1'
        
        data = await self.fetch_data_from_api(session=session, url=url)
        job_list = data.get('data', [])
        if job_list:
            job_id_list.extend([ job['id'] for job in job_list])
        next_link = data.get('links', dict()).get('next', None)

        while next_link:
            url = f'{self.base_url}{next_link}'
            
            data = await self.fetch_data_from_api(session=session, url=url)
            jobs_list = data.get('data', [])
            if job_list:
                job_id_list.extend([ job['id'] for job in job_list])
            next_link = data.get('links', dict()).get('next', None)
            await asyncio.sleep(1.5)
        
        print(f">>> Completed collecting job listings for the position of a {self.category_name}.")
        return job_id_list


    async def fetch_job_description(self, session: aiohttp.ClientSession, job_id: str) -> None:
        """
        Fetch detailed job description using job ID.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use for the request.
            job_id (str): The ID of the job.

        Returns:
            None
        """
        url = f'{self.base_url}/api/v4/jobs/{job_id}'
        data = await self.fetch_data_from_api(session, url)
        job_detail = data.get('job', dict())
        if job_detail:
            coordinate = job_detail.get('address', dict()).get('geo_location', None)
            latitude = coordinate.get('location', dict()).get('lat', None) if coordinate is not None else None
            longitude = coordinate.get('location', dict()).get('lng', None) if coordinate is not None else None
            skills = job_detail.get('skill_tags', None)
            if skills: skills = [ skill['title'] for skill in skills ]
            try:
                job_detail_info = {
                    'job_id': job_id,
                    'platform': self.platform,
                    'category': self.category_name,
                    'url': f'https://www.wanted.co.kr/wd/{job_id}',
                    'company': job_detail['company']['name'],
                    'title': job_detail.get('position', ''),
                    'primary_responsibility': job_detail.get('detail', dict()).get('main_tasks', None),
                    'required': job_detail.get('detail', dict()).get('requirements', None),
                    'preferred': job_detail.get('detail', dict()).get('preferred_points', None),
                    'end_at': job_detail.get('due_time', None),
                    'skills': skills,
                    'location': job_detail.get('address', dict()).get('full_location', None),
                    'welfare': job_detail.get('detail', dict()).get('benefits', None),
                    'body': '',
                    'company_description': job_detail.get('detail', dict()).get('intro', None),
                    'coordinate': [latitude, longitude ] if latitude is not None and longitude is not None else None,
                }
                self.jds.append(job_detail_info)

            except Exception as e:
                print(f'error occured - {e}')
        else:
            pass
        await asyncio.sleep(2)

        
    async def run(self) -> List[Dict]:
        """
        Run the scraping process.

        Returns:
            list: A list of dictionaries containing job details.
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            job_id_list = await self.fetch_job_url_with_async(session=session)
            tasks = [ self.fetch_job_description(session, str(job_id)) for job_id in job_id_list ]
            await asyncio.gather(*tasks)

        return self.jds


    @staticmethod
    def save_json(jobs: List[Dict]) -> str:
        """
        Save job data as JSON.

        Args:
            jobs (list): A list of dictionaries containing job details.

        Returns:
            str: The file path where the JSON file is saved.
        """
        folder_name = 'data'
        if not os.path.exists(folder_name):
            os.mkdir(folder_name) 
        file_name = "wanted.json"
        json_data = {'result': jobs}
        file_path = os.path.join(folder_name, file_name)
        with open(file_path, "w", encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)

        return file_path


    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        """
        Upload a file to AWS S3 bucket.

        Args:
            file_path (str): The path of the file to upload.
            bucket_name (str): The name of the AWS S3 bucket.
            access_key (str): The AWS access key.
            secret_key (str): The AWS secret key.
            region_name (str): The AWS region name.

        Returns:
            None
        """
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        FILE_NAME = f"wanted/year={year}/month={month}/day={day}/wanted.json"
        
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        
        s3.upload_file(file_path, bucket_name, FILE_NAME)
        
        path_name = os.path.join(bucket_name, FILE_NAME)
        print(f"File uploaded successfully to s3://{path_name}")


def main() -> None:

    category = {
            '873': '웹 개발자',
            '10110': '소프트웨어 엔지니어',
            '872': '서버 개발자',
            '669': '프론트엔드 개발자',
            '660': '자바 개발자',
            '900': 'C,C++ 개발자',
            '899': '파이썬 개발자',
            '895': 'Node.js 개발자',
            '1634': '머신러닝 엔지니어',
            '677': '안드로이드 개발자',
            '655': '데이터 엔지니어',
            '674': 'DevOps / 시스템 관리자',
            '678': 'iOS 개발자',
            '665': '시스템,네트워크 관리자',
            '877': '개발 매니저',
            '1024': '데이터 사이언티스트',
            '1026': '기술지원',
            '676': 'QA,테스트 엔지니어',
            '658': '임베디드 개발자',
            '671': '보안 엔지니어',
            '1025': '빅데이터 엔지니어',
            '672': '하드웨어 엔지니어',
            '876': '프로덕트 매니저',
            '1027': '블록체인 플랫폼 엔지니어',
            '10231': 'DBA',
            '893': 'PHP 개발자',
            '10111': '크로스플랫폼 앱 개발자',
            '896': '영상,음성 엔지니어',
            '661': '.NET 개발자',
            '939': '웹 퍼블리셔',
            '10230': 'ERP전문가',
            '898': '그래픽스 엔지니어',
            '795': 'CTO,Chief Technology Officer',
            '10112': 'VR 엔지니어',
            '1022': 'BI 엔지니어',
            '894': '루비온레일즈 개발자',
            '793': 'CIO,Chief Information Officer',
            }
    load_dotenv()
    start = time.time()
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"
    base_url = 'https://www.wanted.co.kr'
    
    jds_list = []
    for category_id, category_name in category.items():
        scraper = Scraper(base_url=base_url, category_id=category_id, category_name=category_name)
        loop = asyncio.get_event_loop()
        jds = loop.run_until_complete(scraper.run())
        jds_list += jds

    file_path = Scraper.save_json(jds_list)
    Scraper.upload_to_s3(
        file_path=file_path,
        bucket_name=bucket_name,
        access_key=access_key,
        secret_key=secret_key,
        region_name=region_name)
    end = time.time()
    print(f"total seconds : {end - start}")


if __name__ == "__main__":
    main()

