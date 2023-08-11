# fastapi 
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles

# built-in
from pathlib import Path
import os
import time 
from typing import List, Dict, Any
from dotenv import load_dotenv

#3rd party
from plugin.rallit_class import Scraper


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
async def scrape_jobs() -> Dict[str, str]:
    try:
        start_time = time.time()
        
        # The main function content from your scraper script
        load_dotenv()

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


