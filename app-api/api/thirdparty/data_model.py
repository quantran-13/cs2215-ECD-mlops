from pydantic import BaseModel


class IExtractDataRequest(BaseModel):
    artifacts_task_id: str
    feature_store_id: str
    export_end_reference_datetime: str = ""
    days_delay: int = 15
    days_export: int = 30


class ITransformDataRequest(BaseModel):
    artifacts_task_id: str


class IValidateDataRequest(BaseModel):
    artifacts_task_id: str


class ILoadDataRequest(BaseModel):
    artifacts_task_id: str
    feature_group_version: str


class IHPORequest(BaseModel):
    artifacts_task_id: str
    forecasting_horizon: int = 24
    k: int = 3
    lag_feature_lag_min: int = 1
    lag_feature_lag_max: int = 72


class ITrainRequest(BaseModel):
    artifacts_task_id: str
    hpo_task_id: str
    forecasting_horizon: int = 24


class IBatchPredictionRequest(BaseModel):
    data_task_id: str
    training_task_id: str
    forecasting_horizon: int = 24
