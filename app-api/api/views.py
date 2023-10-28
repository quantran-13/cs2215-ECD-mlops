from typing import Any, List

from api import schemas
from api.thirparty.clearml_service import ClearMLService
from fastapi import APIRouter

api_router = APIRouter()


@api_router.get("/", status_code=200)
def hello_world() -> dict:
    return {"data": "Hello World"}


@api_router.get("/health", response_model=schemas.Health, status_code=200)
def health() -> dict:
    """
    Health check endpoint.
    """
    return {"name": "Consumer Forecasting API", "api_version": "v1"}


@api_router.get("/consumer_type_values", response_model=schemas.UniqueConsumerType, status_code=200)
def consumer_type_values() -> dict:
    """
    Retrieve unique consumer types.
    """
    # NOTE: hard-code for now
    all_consumer_types = [111, 112, 119, 121, 122, 123, 130, 211, 212, 215, 220, 310, 320, 330, 340, 350, 360, 370, 381, 
                      382, 390, 410, 421, 422, 431, 432, 433, 441, 442, 443, 444, 445, 446, 447, 450, 461, 462, 999]  # fmt: skip
    return {"values": all_consumer_types}


@api_router.get("/area_values", response_model=schemas.UniqueArea, status_code=200)
def area_values() -> List:
    """
    Retrieve unique areas.
    """
    # NOTE: hard-code for now
    return {"values": [1, 2]}


@api_router.get(
    "/predictions/{area}/{consumer_type}",
    response_model=schemas.PredictionResults,
    status_code=200,
)
async def get_predictions(area: int, consumer_type: int) -> Any:
    """
    Get forecasted predictions based on the given area and consumer type.
    """
    # NOTE: hard coded `task_id` for now
    task_id: str = "237e80b4832f46198179c6beae6cf75a"
    task_artifacts = ClearMLService.get_task_artifacts(task_id=task_id)
    # print("list of artifacts: ", task_artifacts.__dict__)

    y = task_artifacts["y"].get()
    y = y.reset_index()

    pred = task_artifacts["predictions"].get()
    pred = pred.reset_index()

    # NOTE: query `consumer_type` & `area`
    y_filtered_df = y[(y["consumer_type"] == consumer_type) & (y["area"] == area)]
    pred_filtered_df = pred[(pred["consumer_type"] == consumer_type) & (pred["area"] == area)]

    preds_datetime_utc = pred_filtered_df["datetime_utc"].to_list()
    datetime_utc = y_filtered_df["datetime_utc"].to_list()

    energy_consumption = y_filtered_df["energy_consumption"].to_list()
    preds_energy_consumption = pred_filtered_df["energy_consumption"].to_list()

    results = {
        "datetime_utc": datetime_utc,
        "energy_consumption": energy_consumption,
        "preds_datetime_utc": preds_datetime_utc,
        "preds_energy_consumption": preds_energy_consumption,
    }
    return results


@api_router.get(
    "/monitoring/metrics",
    response_model=schemas.MonitoringMetrics,
    status_code=200,
)
async def get_metrics() -> Any:
    """
    Get monitoring metrics.
    """
    # # Download the data from GCS.
    # metrics = pd.read_parquet(
    #     f"{get_settings().GCP_BUCKET}/metrics_monitoring.parquet", filesystem=fs
    # )
    # datetime_utc = metrics.index.to_list()
    # mape = metrics["MAPE"].to_list()
    # return {
    #     "datetime_utc": datetime_utc,
    #     "mape": mape,
    # }

    return {
        "datetime_utc": [
            1697381425,
            1697381425,
            1697381425,
            1697381425,
        ],
        "mape": [
            1.0,
            1.0,
            1.0,
            0.8,
        ],
    }


# @api_router.get(
#     "/monitoring/values/{area}/{consumer_type}",
#     response_model=schemas.MonitoringValues,
#     status_code=200,
# )
# async def get_predictions(area: int, consumer_type: int) -> Any:
#     """
#     Get forecasted predictions based on the given area and consumer type.
#     """

#     # Download the data from GCS.
#     y_monitoring = pd.read_parquet(
#         f"{get_settings().GCP_BUCKET}/y_monitoring.parquet", filesystem=fs
#     )
#     predictions_monitoring = pd.read_parquet(
#         f"{get_settings().GCP_BUCKET}/predictions_monitoring.parquet", filesystem=fs
#     )

#     # Query the data for the given area and consumer type.
#     try:
#         y_monitoring = y_monitoring.xs(
#             (area, consumer_type), level=["area", "consumer_type"]
#         )
#         predictions_monitoring = predictions_monitoring.xs(
#             (area, consumer_type), level=["area", "consumer_type"]
#         )
#     except KeyError:
#         raise HTTPException(
#             status_code=404,
#             detail=f"No data found for the given area and consumer typefrontend: {area}, {consumer_type}",
#         )

#     if len(y_monitoring) == 0 or len(predictions_monitoring) == 0:
#         raise HTTPException(
#             status_code=404,
#             detail=f"No data found for the given area and consumer type: {area}, {consumer_type}",
#         )

#     # Prepare data to be returned.
#     y_monitoring_datetime_utc = y_monitoring.index.get_level_values(
#         "datetime_utc"
#     ).to_list()
#     y_monitoring_energy_consumption = y_monitoring["energy_consumption"].to_list()

#     predictions_monitoring_datetime_utc = predictions_monitoring.index.get_level_values(
#         "datetime_utc"
#     ).to_list()
#     predictions_monitoring_energy_consumptionc = predictions_monitoring[
#         "energy_consumption"
#     ].to_list()

#     results = {
#         "y_monitoring_datetime_utc": y_monitoring_datetime_utc,
#         "y_monitoring_energy_consumption": y_monitoring_energy_consumption,
#         "predictions_monitoring_datetime_utc": predictions_monitoring_datetime_utc,
#         "predictions_monitoring_energy_consumptionc": predictions_monitoring_energy_consumptionc,
#     }

#     return results
