# fastapi
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles

# built-in
from pathlib import Path
import os
import time 
from typing import List, Dict, Any
from dotenv import load_dotenv
import asyncio

#3rd party
from plugin.rallit_class import Scraper
from plugin.jobplanet_class import JobPlanetScraper
from plugin.wanted_class import WantedScraper


load_dotenv()

app = FastAPI()


path = "static"
isExist = os.path.exists(path)
if not isExist: os.makedirs(path)
app.mount("/static", StaticFiles(directory="static"), name="static")

# main.py의 위치 
BASE_DIR = Path(__file__).resolve().parent
# print(BASE_DIR)

@app.get("/")
def request_test(request: Request):
    return {"message": "Hello World - FastAPI world"}


@app.get("/api/v1/scrape-rallit")
async def rallit_scrape_jobs() -> Dict[str, str]:
    try:
        start_time = time.time()
        
        # The main function content from your scraper script

        bucket_name = os.getenv('BUCKET_NAME')
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name = "ap-northeast-2"

        job_categories = Scraper.job_category

        data_list: List[Dict[str, Any]] = []
        url = 'https://www.rallit.com/'
        
        for job_category in job_categories:    
            scraper = Scraper(base_url=url, selected_job=job_category)
            scraped_data: List[Dict[str, Any]] = await scraper.get_object_thread(start=1, end=30)
            data_list.extend(scraped_data)

        file_path = Scraper.save_to_json(data_list=data_list)
        Scraper.upload_to_s3(
            file_path=file_path, 
            bucket_name=bucket_name, 
            access_key=access_key, 
            secret_key=secret_key, 
            region_name=region_name
        )
        
        end_time = time.time()
        scraped_time = end_time - start_time
        print(f'took {scraped_time} seconds to scrape {len(data_list)} jobs and upload to S3')
        
        return {"message": f"Scraped {len(data_list)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scrape-jobplanet")
async def jobplanet_scrape_jobs() -> Dict[str, str]:
    try:
        start_time = time.time()
        
        # The main function content from your scraper script

        bucket_name = os.getenv('BUCKET_NAME')
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name = "ap-northeast-2"

        base_url = 'https://www.jobplanet.co.kr/'
        
        tasks = []
        for category_id, category_name in JobPlanetScraper.job_category_dict.items():
            scraper = JobPlanetScraper(base_url=base_url, category_id=category_id, category_name=category_name)
            task = scraper.main()
            tasks.append(task)

        data_list = await asyncio.gather(*tasks)

        result = []
        for data in data_list:
            result.extend(data)

        file_path = JobPlanetScraper.save_json(result)
        
        JobPlanetScraper.upload_to_s3(
            file_path=file_path,
            bucket_name=bucket_name,
            access_key=access_key,
            secret_key=secret_key, 
            region_name=region_name
        )
        
        end_time = time.time()
        scraped_time = end_time - start_time
        print(f'took {scraped_time} seconds to scrape {len(data_list)} jobs and upload to S3')
        
        return {"message": f"Scraped {len(data_list)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/scrape-wanted")
async def wanted_scrape_jobs() -> Dict[str, str]:
    try:
        start_time = time.time()
        
        # The main function content from your scraper script

        bucket_name = os.getenv('BUCKET_NAME')
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name = "ap-northeast-2"

        base_url = 'https://www.wanted.co.kr'
        
        jds_list = []
        for category_id, category_name in WantedScraper.category.items():
            scraper = WantedScraper(base_url=base_url, category_id=category_id, category_name=category_name)
            jds: List[Dict[str, Any]] = await scraper.run()
            jds_list += jds

        file_path = WantedScraper.save_json(jds_list)
        WantedScraper.upload_to_s3(
            file_path=file_path,
            bucket_name=bucket_name,
            access_key=access_key,
            secret_key=secret_key,
            region_name=region_name)

        end_time = time.time()
        scraped_time = end_time - start_time

        print(f'took {scraped_time} seconds to scrape {len(jds_list)} jobs and upload to S3')
        
        return {"message": f"Scraped {len(jds_list)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

