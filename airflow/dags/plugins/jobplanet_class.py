import json
import math
import asyncio

from datetime import datetime
from typing import Any, Dict, List


class Scraper:
    def __init__(self, base_url: str, category_id: int, category_name: str) -> None:
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
