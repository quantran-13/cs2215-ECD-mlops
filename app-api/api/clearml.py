# import gcsfs

from api.thirparty.clearml_service import ClearMLService
from clearml import Task
from fastapi import APIRouter

clearml_router = APIRouter()


@clearml_router.get("/", status_code=200)
def hello_world() -> dict:
    return {"data": "Hello World"}

from pydantic import BaseModel
class IExtractDataRequest(BaseModel): 
    export_end_reference_datetime: str
    days_delay: int
    days_export: int
    artifacts_task_id: str
    feature_store_id: str


@clearml_router.post("/feature_pipeline/extract", status_code=200)
def run_extract_data(req: IExtractDataRequest) -> dict:
    task = ClearMLService.get_extract_task(
        args = [
            # "artifacts_task_id": "OVERWRITE_ME",
            ("export_end_reference_datetime", req.export_end_reference_datetime),
            ("days_delay", req.days_delay),
            ("days_export", req.days_export),
            ("artifacts_task_id", req.artifacts_task_id),
            ("feature_store_id", req.feature_store_id),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return {"task_id": task.id}


@clearml_router.get("/feature_pipeline/transform", status_code=200)
def run_transform_data() -> dict:
    task = ClearMLService.get_transform_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return {"task_id": task.id}


@clearml_router.get("/feature_pipeline/load", status_code=200)
def run_load_data() -> dict:
    task = ClearMLService.get_load_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return {"task_id": task.id}


@clearml_router.get("/feature_pipeline/validate", status_code=200)
def run_validate_data():
    task = ClearMLService.get_validate_task()
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    return {"task_id": task.id}
