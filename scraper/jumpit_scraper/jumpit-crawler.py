from typing import List, Dict
import math
import requests
import json
import os
import time
import boto3
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# 대략 15분 소요됨
class Scraper:
    def __init__(self, category_id: int, category_name: str):
        self.category_id = category_id
        self.category_name = category_name


    def scrape_category(self) -> List[Dict]:
        """
        카테고리별 공고를 크롤링
        """
        position_ids = self.get_position_ids()
        data_list = []

        # position_id 리스트를 순회하며 공고 상세 정보를 가져옴
        for i, position_id in enumerate(position_ids):

            # too many requests 방지를 위해 100개마다 1초 쉼
            if i % 100 == 0:
                time.sleep(1)

            print(f'{self.category_name} - {i}')
            position_dict = self.get_position_detail(position_id)
            if not position_dict:
                continue
            data_list.append(position_dict)
        return data_list


    def get_category_page_count(self) -> int:
        """
        카테고리별 공고의 페이지 수를 반환
        """
        response = requests.get(f'https://api.jumpit.co.kr/api/positions?page=1&jobCategory={self.category_id}')
        json_data = response.json()
        total_count = json_data['result']['totalCount']
        print(f'{self.category_name} - total count: {total_count}')
        return math.ceil(total_count / 16)


    def get_position_ids(self) -> List[int]:
        """
        카테고리에 속한 공고들의 id 리스트를 반환
        """
        page_count = self.get_category_page_count()
        position_ids = []

        for i in range(1, page_count + 1):
            response = requests.get(f'https://api.jumpit.co.kr/api/positions?page={i}&jobCategory={self.category_id}')
            json_data = response.json()

            positions = json_data['result']['positions']
            position_ids += [position['id'] for position in positions]

        return position_ids


    def get_position_detail(self, position_id: int) -> Dict:
        """
        공고 상세 정보를 반환
        """
        response = requests.get(f'https://api.jumpit.co.kr/api/position/{position_id}')
        if response.status_code != 200:
            print(f'Error: {response.status_code}')
            return None

        result = response.json()['result']
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

        return position_dict


    @staticmethod
    def save_to_json(data_list:list):
        folder = 'data'
        filename = 'jumpit.json'
        """Save the data to JSON file """
        # JSON 파일로 저장
        json_data = {'result': data_list}

        # print(f'scraped {len(data_list)} data')

        if not os.path.exists(folder):
            os.makedirs(folder)


        with open(os.path.join(folder, filename), 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")


    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        '''Uploads the specified file to an AWS S3 bucket.'''

        print("Start upload!")

        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        FILE_NAME = f"jumpit/year={year}/month={month}/day={day}/jumpit.json"

        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)

        s3.upload_file(file_path, bucket_name, FILE_NAME)

        path_name = os.path.join(bucket_name, FILE_NAME)
        print(f"End Upload to s3://{path_name}")



def main():
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

    start_time = time.time()

    data_list = []
    for category_id, category_name in job_category_dict.items():
        print(f'start scraping {category_id} - {category_name}')
        scraper = Scraper(category_id, category_name)
        data_list += scraper.scrape_category()

    end_time = time.time()
    print(f'elapsed time: {end_time - start_time}')

    Scraper.save_to_json(data_list)


    # json 파일을 s3에 업로드
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"

    Scraper.upload_to_s3('data/jumpit.json', bucket_name, access_key, secret_key, region_name)


if __name__ == '__main__':
    main()
