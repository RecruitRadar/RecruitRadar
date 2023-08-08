import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import aiohttp
import asyncio
import time
import json
import os
import boto3
from dotenv import load_dotenv
from datetime import date


class Scraper:    
    '''
    A web scraper class for extracting job listings from "rallit.com".
    
    Attributes:
        base_url: The base URL for the website to be scraped.
        selected_job: The job category to be scraped.
        jobs: A list to store the scraped job listings.
        user_agent_list: A list of user agents to randomize headers for web requests.
        headers: Headers for the web requests.
    '''
    def __init__(self, base_url:str, selected_job:str) -> None:
        
        '''Initializes the Scraper with the given base URL and job category.'''

        self.base_url = base_url
        self.selected_job = selected_job
        self.jobs = list() # {"result": list()}
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
        
        self.headers = {'User-Agent': random.choice(self.user_agent_list)}
    

    async def get_object_thread(self, start:int, end:int) -> list:
        '''
        Generates an aiohttp session and asynchronously scrapes job listings 
        between the provided start and end page numbers.
        '''
        print('start scrape - http session generate')
        async with aiohttp.ClientSession() as session:  
            tasks = []
            for p_index in range(start, end+1):
                task = asyncio.ensure_future(
                    self.get_job_parser(base_url=self.base_url, p_index=p_index, session=session)
                    )
                tasks.append(task)
            await asyncio.gather(*tasks)
        
        return self.jobs
    

    async def get_job_parser(self, base_url:str, p_index:int, session) -> None:
        '''
        Asynchronously parses the job listings on a given page and extracts relevant details.
        '''
        url = f'{base_url}?job={self.selected_job}&jobGroup=DEVELOPER&pageNumber={p_index}'
        headers = self.headers
        
        async with session.get(url, headers=headers) as response:
            try : 
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                ul_set = soup.find(class_='css-mao678')
                rows = ul_set.find_all('li')
                for row in rows:
                    a_tag = row.find('a')
                    # print(a_tag)
                    if a_tag:
                        position_url = a_tag.attrs['href']

                        try:
                            import re
                            position_key_match = re.search(r'/positions/(\d+)', position_url)
                            position_key = position_key_match.group(1)

                        except:
                            print("No match position_key found")
                                            
                        position_dict = await self.get_position_parser(position_key=position_key, session=session)
                        # self.jobs["result"].append(position_dict)
                        self.jobs.append(position_dict)
                    else:
                        continue
                # print(f'{url} -> scraped {len(self.jobs)} data')

            except:
                pass
                
                
   
    async def get_position_parser(self, position_key:str, session) -> dict:
        '''
        Asynchronously fetches details of a specific job position using its key.
        '''
        # https://www.rallit.com/api/v1/position/{position_key}
        position_url = f'https://www.rallit.com/api/v1/position/{position_key}'
        print(position_url)
        headers = self.headers
        position_dict = dict()
        PLATFORM_NAME = "rallit"
        
        async with session.get(position_url, headers=headers) as response:
            
            data = await response.json()
            # print(data)
            data_detail = data.get('data', None)

            JOB_ID = data_detail.get('id', None)
            
            position_dict["job_id"] = JOB_ID
            position_dict["category"] = self.selected_job
            position_dict["platform"] = PLATFORM_NAME
            position_dict["company"] = data_detail.get('companyName', None)
            position_dict["title"] = data_detail.get('title', None)

            
            position_dict["preferred"] = data_detail.get('preferredQualifications', None)
            position_dict["required"] = data_detail.get('basicQualifications', None)
            position_dict["primary_responsibility"] = data_detail.get('responsibilities', None)

            
            position_dict["url"] = "https://www.rallit.com/positions/" + str(JOB_ID)
            position_dict["end_at"] = data_detail.get('endedAt', None)

            position_dict["skills"] = data_detail.get('jobSkillKeywords', None)
            position_dict["location"] = data_detail.get('addressMain', None)
            position_dict["region"] = data_detail.get('addressRegion', dict()).get("name", None)
            
            position_dict["body"] = data_detail.get('description', None)
            
            position_dict["company_id"] = data_detail.get('companyId', None)
            position_dict["company_description"] = data_detail.get('companyDescription', None)
            position_dict["status"] = data_detail.get('status', dict()).get("code", None)
            latitude = data_detail.get('latitude', None)
            longitude = data_detail.get('longitude', None)
            position_dict["coordinate"] = [latitude, longitude] if latitude is not None and longitude is not None else None
        
            # print(position_dict)
        return position_dict 
    
    
    @staticmethod
    def paragraph_parsing(parsing_data:str):
        '''
        Strips and extracts the first paragraph from the provided parsing data.
        '''
        paragraphs = parsing_data.find_all('p')       
        paragraphs = [par.text.strip() for par in paragraphs]
        # print(paragraphs[0])
        # print('------')
        return paragraphs[0]
    
    
    def save_to_csv(self, filename="rallit.csv"):
        """Save the data to a CSV file."""
        df = pd.DataFrame(self.jobs)
        
        # If 'company_name' column is present in the DataFrame, reorder the columns
        if 'company_name' in df.columns:
            columns = ['company_name'] + [col for col in df if col != 'company_name']
            df = df[columns]
            
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"Data saved to {filename}")
    
    
    @staticmethod
    def save_to_json(data_list:list) -> str:
        '''Saves the provided list of data to a JSON file.'''

        folder = 'data'
        filename = 'rallit.json'
        """Save the data to JSON file """
        # JSON 파일로 저장
        json_data = {'result': data_list}
        
        # print(f'scraped {len(data_list)} data')
        file_path = os.path.join(folder, filename)
        
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(json_data, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")
        
        return file_path
            
    
    
    @staticmethod
    def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
        '''Uploads the specified file to an AWS S3 bucket.'''

        print("Start upload!")
        
        today = date.today()
        year = str(today.year)
        month = str(today.month).zfill(2)
        day = str(today.day).zfill(2)
        FILE_NAME = f"rallit/year={year}/month={month}/day={day}/rallit.json"
        
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
        
        s3.upload_file(file_path, bucket_name, FILE_NAME)
        
        path_name = os.path.join(bucket_name, FILE_NAME)
        print(f"End Upload to s3://{path_name}")



def main():
    # load .env
    # docker로 래핑하면 환경변수로 읽어와야 한다.
    load_dotenv()
    
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"

    job_categories = [
        "BACKEND_DEVELOPER", 
        "FRONTEND_DEVELOPER",
        "SOFTWARE_ENGINEER",
        "ANDROID_DEVELOPER",
        "IOS_DEVELOPER",
        "CROSS_PLATFORM_DEVELOPER",
        "DATA_ENGINEER",
        "DATA_SCIENTIST",
        "DATA_ANALYST",
        "MACHINE_LEARNING",
        "DBA",
        "DEV_OPS",
        "INFRA_ENGINEER",
        "QA_ENGINEER",
        "SUPPORT_ENGINEER",
        "SECURITY_ENGINEER",
        "BLOCKCHAIN_ENGINEER",
        "HARDWARE_EMBEDDED_ENGINEER",
        "AGILE_SCRUM_MASTER"
    ]
    
    start_time = time.time()
    
    data_list = list()
    for job_category in job_categories:
        print(job_category)
        time.sleep(1)
        url = 'https://www.rallit.com/'
        scraper = Scraper(base_url=url, selected_job=job_category)
        # list 반환
        scraped_data = asyncio.run(scraper.get_object_thread(start=1, end=30))
        data_list.extend(scraped_data)
    
    
    print(len(data_list))
    file_path = Scraper.save_to_json(data_list=data_list)
    Scraper.upload_to_s3(file_path = file_path, bucket_name=bucket_name, 
                         access_key=access_key, secret_key=secret_key, 
                         region_name=region_name)


    print("--- %s seconds ---" % (time.time() - start_time))
    

if __name__ == '__main__':
    main()
    