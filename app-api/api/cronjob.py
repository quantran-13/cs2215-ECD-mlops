# import gcsfs

from fastapi import APIRouter, HTTPException

from .clearml_service import ClearMLService


trigger_api_router = APIRouter()

@trigger_api_router.get("/", status_code=200)
def hello_world() -> dict:
    return {"data": "Hello World"}



@trigger_api_router.get("/", status_code=200)
def trigger_extract_data(): 
    return  