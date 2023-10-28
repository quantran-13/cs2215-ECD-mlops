from pydantic import BaseModel


class IExtractDataRequest(BaseModel):
    artifacts_task_id: str
    feature_store_id: str
    export_end_reference_datetime: str = ""
    days_delay: int
    days_export: int


class ITransformDataRequest(BaseModel):
    artifacts_task_id: str


class IValidateDataRequest(BaseModel):
    artifacts_task_id: str


class ILoadDataRequest(BaseModel):
    artifacts_task_id: str
    feature_group_version: str


class IHPORequest(BaseModel):
    artifacts_task_id: str
    forecasting_horizon: int
    k: int
    lag_feature_lag_min: int
    lag_feature_lag_max: int


class ITrainRequest(BaseModel):
    artifacts_task_id: str
    hpo_task_id: str
    forecasting_horizon: int


class IBatchPredictionRequest(BaseModel):
    data_task_id: str
    training_task_id: str
    forecasting_horizon: int
