import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import aiohttp
import asyncio
import time
import json


class Scraper:    
    def __init__(self, flag:str, base_url:str, selected_job:str) -> None:
        self.flag = flag
        self.base_url = base_url
        self.selected_job = selected_job
        self.jobs = dict()
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
        
        self.headers = {'User-Agent': random.choice(self.user_agent_list)}
    

    async def get_object_thread(self, start:int, end:int) -> None:
        '''
        시스템의 DNS resolver를 사용하는 세션을 만듭니다.
        '''
        print('start scrape - http session generate')
        async with aiohttp.ClientSession() as session:  
            tasks = []
            for p_index in range(start, end+1):
                time.sleep(1)
                if self.flag == 'rallit':
                    task = asyncio.ensure_future(self.get_job_parser(base_url=self.base_url, p_index=p_index, session=session))

                tasks.append(task)
            await asyncio.gather(*tasks)
    

    async def get_job_parser(self, base_url:str, p_index:int, session) -> None:

        url = f'{base_url}?job={self.selected_job}&jobGroup=DEVELOPER&pageNumber={p_index}'
        
        print(url)
        headers = self.headers
        
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            ul_set = soup.find(class_='css-mao678')
            # print(table)
            rows = ul_set.find_all('li')
            
            # print(rows)
            for row in rows:
            
                a_tag = row.find('a')
                # print(a_tag)
                if a_tag:
                    
                    position_url = a_tag.attrs['href']
                    
                    try:
                        import re
                        position_key_match = re.search(r'/positions/(\d+)', position_url)
                        position_key = position_key_match.group(1)
                        # if position_key:
                        #     print(position_key)
                    except:
                        print("No match position_key found")
                    
                    company_name = row.find(class_= 'summary__company-name css-x5ccem').get_text()
                    
                    target_url = base_url + position_url
                    
                    position_dict = await self.get_position_parser(url=target_url, session=session)
                    position_dict["company_name"] = company_name
                    position_dict["position_key"] = position_key
                    position_dict["target_url"] = target_url
                    
                    self.jobs[position_key] = position_dict
                else:
                    continue

        # print(self.jobs[0])
        print(f'scraped {len(self.jobs)} data')


    async def get_position_parser(self, url:str, session) -> dict:
        

        position_url = url
        print(position_url)
        headers = self.headers
        position_dict = dict()
        
        async with session.get(position_url, headers=headers) as response:
        
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            
            header = soup.find(class_='css-17ueevk')
            position_name = header.get_text()
            position_dict["position_name"] = position_name

            table = soup.find(class_='css-1vbiael') # 잡에 대한 글이 담긴 클래스
            rows = table.find_all('section')
            
            # table header 제거
            intro = rows[0]
            paragraphs = intro.find_all('p')
            # print(paragraphs)
            
            
            paragraphs = [par.text.strip() for par in paragraphs]
            position_dict["intro"] = paragraphs[0]
            
            works = rows[1]
            work_intro = works.find_all(class_='css-15i1vgz')       
            
            main_work = Scraper.paragraph_parsing(work_intro[0])
            position_dict["main_work"] = main_work
            
            require = Scraper.paragraph_parsing(work_intro[1])
            position_dict["require"] = require
            
            preference = Scraper.paragraph_parsing(work_intro[2])
            position_dict["preference"] = preference
            
            welfare = Scraper.paragraph_parsing(work_intro[3])
            position_dict["welfare"] = welfare
            
            
            # css-104fbyc
            skill_set_class = soup.find(class_='css-104fbyc')
            skill_rows = skill_set_class.find_all(class_='css-kgsirb')
            skills = [skill.text.replace("#<!-- -->", "").replace("#", "").strip() for skill in skill_rows]
            # print(skills)
            position_dict["skills"] = skills
            
        return position_dict 
    
    
    @staticmethod
    def paragraph_parsing(parsing_data:str):
        """ strip the parsed data"""
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
    
    
    def save_to_json(self, filename="rallit.json"):
        """Save the data to JSON file """
        # JSON 파일로 저장
        with open(filename, 'w', encoding='utf-8') as json_file:
            json.dump(self.jobs, json_file, ensure_ascii=False, indent=4)
            print(f"Data saved to {filename}")

        

if __name__ == '__main__':
    selected_job = 'DATA_ENGINEER'
    url = 'https://www.rallit.com/'
    scraper = Scraper(flag='rallit', base_url=url, selected_job=selected_job)
    position_list = asyncio.run(scraper.get_object_thread(start=1, end=2))
    scraper.save_to_json()
    