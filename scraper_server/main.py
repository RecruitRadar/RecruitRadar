import uvicorn
from pathlib import Path

from fastapi import FastAPI, Request

app = FastAPI()

# app.mount("/static", StaticFiles(directory="static"), name="static")
# main.py의 부모 폴더
BASE_DIR = Path(__file__).resolve().parent


if __name__ == "__main__":
    uvicorn.run('app.main:app', host='localhost', port=8000, reload=True)