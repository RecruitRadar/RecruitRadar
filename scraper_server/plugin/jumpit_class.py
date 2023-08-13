from typing import List, Dict
import math
import requests
import json
import os
import time
import aiohttp
import asyncio
import random
import boto3
from datetime import date, datetime
from dotenv import load_dotenv


class JumpitScraper:

    job_category_dict = {
        1: '서버/백엔드 개발자',
        2: '프론트엔드 개발자',
        3: '웹 풀스택 개발자',
        4: '안드로이드 개발자',
        5: '게임 클라이언트 개발자',
        6: '게임 서버 개발자',
        7: '빅데이터 엔지니어,DBA',
        8: '인공지능/머신러닝',
        9: 'devops/시스템 엔지니어',
        10: '정보보안 담당자',
        11: 'QA 엔지니어',
        12: '인공지능/머신러닝,개발 PM',
        13: 'HW/임베디드',
        14: 'etc',
        15: 'SW/솔루션',
        16: 'IOS 개발자',
        17: '웹퍼블리셔',
        18: '크로스플랫폼 앱개발자',
        19: '빅데이터 엔지니어',
        20: 'VR/AR/3D,게임 클라이언트 개발자',
        21: '기술지원',
        22: '프론트엔드 개발자,블록체인'
    }

    def __init__(self, category_id: int, category_name: str):
        self.category_id = category_id
        self.category_name = category_name
        self.jobs = []
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        ]
        self.headers = {'User-Agent': random.choice(self.user_agent_list)}


    async def scrape_category(self) -> List[Dict]:
        """
        카테고리별 공고를 크롤링
        """
        async with aiohttp.ClientSession() as session:
            position_ids = await self.get_position_ids(session)

            tasks = []
            for i, position_id in enumerate(position_ids):

                print(f'{self.category_name} - {i}')

                task = asyncio.create_task(
                    self.get_position_detail(position_id, session)
                )
                tasks.append(task)
            await asyncio.gather(*tasks)

        return self.jobs


    async def get_category_page_count(self, session) -> int:
        """
        카테고리별 공고의 페이지 수를 반환
        """
        url = f'https://api.jumpit.co.kr/api/positions?page=1&jobCategory={self.category_id}'
        async with session.get(url, headers=self.headers) as response:
            json_data = await response.json()
            total_count = json_data['result']['totalCount']
            print(f'{self.category_name} - total count: {total_count}')
            return math.ceil(total_count / 16)


    async def get_position_ids(self, session) -> List[int]:
        """
        카테고리에 속한 공고들의 id 리스트를 반환
        """
        page_count = await self.get_category_page_count(session)
        position_ids = []

        for i in range(1, page_count + 1):
            url = f'https://api.jumpit.co.kr/api/positions?page={i}&jobCategory={self.category_id}'
            async with session.get(url, headers=self.headers) as response:
                try:
                    json_data = await response.json()
                    positions = json_data['result']['positions']
                    position_ids.extend([position['id'] for position in positions])
                except:
                    pass

        return position_ids


    async def get_position_detail(self, position_id: int, session) -> Dict:
        """
        공고 상세 정보를 반환
        """
        url = f'https://api.jumpit.co.kr/api/position/{position_id}'
        async with session.get(url, headers=self.headers) as response:
            if response.status != 200:
                print(f'Error: {response.status}')
                return None

            response_json = await response.json()
            result = response_json['result']
            position_dict = dict()

            position_dict['job_id'] = position_id
            position_dict['platform'] = 'jumpit'
            position_dict['category'] = self.category_name
            position_dict['company'] = result['companyName']
            position_dict['title'] = result['title']
            position_dict['preferred'] = result['preferredRequirements']
            position_dict['required'] = result['qualifications']
            position_dict['primary_responsibility'] = result['responsibility']
            position_dict['url'] = f'https://www.jumpit.co.kr/position/{position_id}'
            position_dict['end_at'] = result['closedAt']
            position_dict['skills'] = [techStack['stack'] for techStack in result['techStacks']]
            position_dict['location'] = result['jobPostingForSearchEngine']['jobLocation']['address']['streetAddress']
            position_dict['welfare'] = result['welfares']
            position_dict['body'] = None
            position_dict['company_description'] = result['serviceInfo']
            position_dict['coordinate'] = None

        self.jobs.append(position_dict)


    @staticmethod
    def save_to_json(data_list: List[Dict]):
        """Save the data to JSON file """

        folder = 'static'
        filename = 'jumpit.json'

        # JSON 파일로 저장
        json_data = {'result': data_list}
        file_path = os.path.join(folder, filename)

        with open(os.path.join(folder, filename), 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")

        return file_path


    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        '''Uploads the specified file to an AWS S3 bucket.'''

        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        file_name = f"jumpit/year={year}/month={month}/day={day}/jumpit.json"

        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        s3.upload_file(file_path, bucket_name, file_name)

        path_name = os.path.join(bucket_name, file_name)
        print(f"End Upload to s3://{path_name}")
