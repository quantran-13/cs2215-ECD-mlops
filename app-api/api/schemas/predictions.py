from pydantic import BaseModel


class PredictionResults(BaseModel):
    datetime_utc: list[str]
    energy_consumption: list[float]
    preds_datetime_utc: list[str]
    preds_energy_consumption: list[float]


class MonitoringMetrics(BaseModel):
    datetime_utc: list[int]
    mape: list[float]


class MonitoringValues(BaseModel):
    y_monitoring_datetime_utc: list[int]
    y_monitoring_energy_consumption: list[float]
    predictions_monitoring_datetime_utc: list[int]
    predictions_monitoring_energy_consumptionc: list[float]
