# fastapi
from fastapi import FastAPI, Request

# built-in
from pathlib import Path
from typing import List, Dict, Any
from api import scraper


app = FastAPI()
app.include_router(scraper.router)

# main.py의 위치
BASE_DIR = Path(__file__).resolve().parent

    
@app.get("/")
async def request_test(request: Request) -> Dict[str, str]:
    return {"message": "Hello World - FastAPI world"}


