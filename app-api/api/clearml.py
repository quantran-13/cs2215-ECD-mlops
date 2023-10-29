from api.thirdparty import data_model as idm
from api.thirdparty.clearml_service import ClearMLService
from clearml import Task
from fastapi import APIRouter

clearml_router = APIRouter()


@clearml_router.get("/", status_code=200)
def hello_world() -> dict:
    return {"data": "Hello World"}


@clearml_router.get("/v1/status", status_code=200)
def get_task_status(
    task_id: str, 
    project_name: str | None = "cs2215-project", 
) -> dict:
    task = Task.get_task(
        task_id=task_id, 
        project_name=project_name
    )
    status = task.get_status()
    return { 
        "task_id": task.id, 
        "status": status
    } 

@clearml_router.get("/v1/get_metadata", status_code=200)
def get_task_status(
    task_id: str, 
    project_name: str | None = "cs2215-project", 
) -> dict:
    task = Task.get_task(
        task_id=task_id, 
        project_name=project_name
    )
    status = task.get_status()
    metadata = task.artifacts['metadata'].get('url')
    return { 
        "task_id": task.id, 
        "status": status, 
        "metadata": metadata, 
    } 

@clearml_router.post("/v1/feature_pipeline/extract", status_code=200)
def run_extract_data(req: idm.IExtractDataRequest) -> dict:
    task = ClearMLService.get_extract_task(
        args=[
            ("artifacts_task_id", req.artifacts_task_id),
            ("feature_store_id", req.feature_store_id),
            ("export_end_reference_datetime", req.export_end_reference_datetime),
            ("days_delay", req.days_delay),
            ("days_export", req.days_export),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/feature_pipeline/transform", status_code=200)
def run_transform_data(req: idm.ITransformDataRequest) -> dict:
    task = ClearMLService.get_transform_task(
        args=[
            ("artifacts_task_id", req.artifacts_task_id),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/feature_pipeline/validate", status_code=200)
def run_validate_data(req: idm.IValidateDataRequest) -> dict:
    task = ClearMLService.get_validate_task(
        args=[
            ("artifacts_task_id", req.artifacts_task_id),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/feature_pipeline/load", status_code=200)
def run_load_data(req: idm.ILoadDataRequest) -> dict:
    task = ClearMLService.get_load_task(
        args=[("artifacts_task_id", req.artifacts_task_id), ("feature_group_version", req.feature_group_version)]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/training_pipeline/hpo", status_code=200)
def run_hpo(req: idm.IHPORequest) -> dict:
    task = ClearMLService.get_hpo_task(
        args=[
            ("artifacts_task_id", req.artifacts_task_id),
            ("forecasting_horizon", req.forecasting_horizon),
            ("k", req.k),
            ("lag_feature_lag_min", req.lag_feature_lag_min),
            ("lag_feature_lag_max", req.lag_feature_lag_max),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/training_pipeline/train", status_code=200)
def run_train(req: idm.ITrainRequest) -> dict:
    task = ClearMLService.get_train_task(
        args=[
            ("artifacts_task_id", req.artifacts_task_id),
            ("hpo_task_id", req.hpo_task_id),
            ("forecasting_horizon", req.forecasting_horizon),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}


@clearml_router.post("/v1/batch_prediction_pipeline/batch_prediction", status_code=200)
def run_batch_prediction(req: idm.IBatchPredictionRequest) -> dict:
    task = ClearMLService.get_batch_prediction_task(
        args=[
            ("data_task_id", req.data_task_id),
            ("training_task_id", req.training_task_id),
            ("forecasting_horizon", req.forecasting_horizon),
        ]
    )
    Task.enqueue(
        task=task,
        queue_name="default",
    )
    task_status = task.get_status()
    return {"task_id": task.id, "task_status": task_status}
