from fastapi.testclient import TestClient
from fastapi import FastAPI, HTTPException

from fastapi import status
from main import app
from api.scraper import router

from plugin.rallit_class import Scraper

# import json
# import pytest
# from unittest.mock import patch
# from unittest.mock import AsyncMock


client = TestClient(app)


def test_request_test():
    response = client.get("/")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"message": "Hello World - FastAPI world"}


# def test_rallit_scrape_jobs():

#     with patch('router.get_secret') as mock_get_secret:
#         mock_get_secret.return_value = ("BUCKET_NAME", "ACCESS_KEY", "SECRET_KEY", "REGION_NAME")

#         with patch('Scraper.get_object_thread', new_callable=AsyncMock) as mock_get_object_thread, \
#              patch('Scraper.save_to_json') as mock_save_to_json, \
#              patch('Scraper.upload_to_s3') as mock_upload_to_s3:

#             mock_get_object_thread.return_value = [{} for _ in range(5)]
#             mock_save_to_json.return_value = 'mock_file_path'
#             mock_upload_to_s3.return_value = None

#             client = TestClient(app)
#             response = client.get("/api/v1/scrape-rallit")

#             print(response.json())  # Print the response for debugging
            
#             assert response.status_code == 200
#             assert "message" in response.json()
#             assert "Scraped" in response.json()["message"]

