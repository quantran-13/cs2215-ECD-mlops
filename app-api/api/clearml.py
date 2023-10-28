# import gcsfs

from clearml import Task
from fastapi import APIRouter, HTTPException

from api.thirparty.clearml_service import ClearMLService


clearml_router = APIRouter()

@clearml_router.get("/", status_code=200)
def hello_world() -> dict:
    return {"data": "Hello World"}



@clearml_router.get("/feature_pipeline/extract", status_code=200)
def run_extract_data() -> dict: 
    task = ClearMLService.get_extract_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return { 
        "task_id": task.id
    } 


@clearml_router.get("/feature_pipeline/transform", status_code=200)
def run_transform_data() -> dict: 
    task = ClearMLService.get_transform_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return { 
        "task_id": task.id
    } 

@clearml_router.get("/feature_pipeline/load", status_code=200)
def run_load_data() -> dict: 
    task = ClearMLService.get_load_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return { 
        "task_id": task.id
    } 

@clearml_router.get("/feature_pipeline/validate", status_code=200)
def run_validate_data(): 
    task = ClearMLService.get_validate_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return { 
        "task_id": task.id
    } 


