from fastapi import FastAPI, Body, HTTPException, Depends, APIRouter
from fastapi.staticfiles import StaticFiles

# built-in
import os
import json
import time
import boto3
import asyncio
from typing import List, Dict, Any

from botocore.exceptions import ClientError

#3rd party
from plugin.rallit_class import Scraper
from plugin.jobplanet_class import JobPlanetScraper
from plugin.wanted_class import WantedScraper
from plugin.jumpit_class import JumpitScraper


router = APIRouter(prefix="/api/v1")

path = "static"
isExist = os.path.exists(path)
if not isExist: os.makedirs(path)
router.mount("/static", StaticFiles(directory="static"), name="static")

def get_secret():
    """
    AWS Secrets Manager를 이용해 환경변수를 불러옵니다.
    """
    secret_name = "prod/de-1-1/back-end"
    REGION_NAME = "ap-northeast-2"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=REGION_NAME
    )
    
    print(client.list_secrets())
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    BUCKET_NAME = secret_dict['BUCKET_NAME']
    ACCESS_KEY = secret_dict['AWS_ACCESS_KEY_ID']
    SECRET_KEY = secret_dict['AWS_SECRET_ACCESS_KEY']

    return BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME


@router.get("/scrape-rallit")
async def rallit_scrape_jobs() -> Dict[str, str]:
    
    try: BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME = get_secret()
    except ClientError as e: raise HTTPException(status_code=500, detail=f"AWS ClientError: {e}")    
    
    try:
        start_time = time.time()

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
            bucket_name=BUCKET_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region_name=REGION_NAME
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        print(f'took {scraped_time} seconds to scrape {len(data_list)} jobs and upload to S3')

        return {"message": f"Scraped {len(data_list)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scrape-jobplanet")
async def jobplanet_scrape_jobs() -> Dict[str, str]:
    
    try: BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME = get_secret()
    except ClientError as e: raise HTTPException(status_code=500, detail=f"AWS ClientError: {e}")       
    try:
        start_time = time.time()

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
            bucket_name=BUCKET_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region_name=REGION_NAME
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        print(f'took {scraped_time} seconds to scrape {len(result)} jobs and upload to S3')

        return {"message": f"Scraped {len(result)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scrape-wanted")
async def wanted_scrape_jobs() -> Dict[str, str]:
    
    try: BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME = get_secret()
    except ClientError as e: raise HTTPException(status_code=500, detail=f"AWS ClientError: {e}")   
        
    try:
        start_time = time.time()

        base_url = 'https://www.wanted.co.kr'

        jds_list = []
        for category_id, category_name in WantedScraper.category.items():
            scraper = WantedScraper(base_url=base_url, category_id=category_id, category_name=category_name)
            jds: List[Dict[str, Any]] = await scraper.run()
            jds_list += jds

        file_path = WantedScraper.save_json(jds_list)
        WantedScraper.upload_to_s3(
            file_path=file_path,
            bucket_name=BUCKET_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region_name=REGION_NAME
            )

        end_time = time.time()
        scraped_time = end_time - start_time

        print(f'took {scraped_time} seconds to scrape {len(jds_list)} jobs and upload to S3')

        return {"message": f"Scraped {len(jds_list)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scrape-jumpit")
async def jumpit_scrape_jobs() -> Dict[str, str]:
    
    try: BUCKET_NAME, ACCESS_KEY, SECRET_KEY, REGION_NAME = get_secret()
    except ClientError as e: raise HTTPException(status_code=500, detail=f"AWS ClientError: {e}")   
        
    try:
        start_time = time.time()

        tasks = []
        for category_id, category_name in JumpitScraper.job_category_dict.items():
            scraper = JumpitScraper(category_id, category_name)
            task = scraper.scrape_category()
            tasks.append(task)

        data_list = await asyncio.gather(*tasks)
        result = []
        for data in data_list:
            result.extend(data)

        file_path = JumpitScraper.save_to_json(result)

        JumpitScraper.upload_to_s3(
            file_path=file_path,
            bucket_name=BUCKET_NAME,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            region_name=REGION_NAME
        )

        end_time = time.time()
        scraped_time = end_time - start_time
        print(f'took {scraped_time} seconds to scrape {len(data_list)} jobs and upload to S3')

        return {"message": f"Scraped {len(result)} jobs and uploaded to S3 successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
