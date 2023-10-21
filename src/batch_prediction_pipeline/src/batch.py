import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

import time
from clearml import Dataset

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR
import pandas as pd
from src.batch_prediction_pipeline.src.data import load_data
from src.utils.task_utils import get_task_artifacts, save_json
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def predict(task, data_task_id: str, training_task_id: str, fh: int = 24):
    """Main function used to do batch predictions.

    Args:
        fh (int, optional): forecast horizon. Defaults to 24.
        feature_view_version (Optional[int], optional): feature store feature view version. If None is provided, it will try to load it from the cached feature_view_metadata.json file.
        model_version (Optional[int], optional): model version to load from the model registry. If None is provided, it will try to load it from the cached train_metadata.json file.
        start_datetime (Optional[datetime], optional): start datetime used for extracting features for predictions. If None is provided, it will try to load it from the cached feature_pipeline_metadata.json file.
        end_datetime (Optional[datetime], optional): end datetime used for extracting features for predictions. If None is provided, it will try to load it from the cached feature_pipeline_metadata.json file.
    """
    logger.info("Loading data from feature store...")
    t1 = time.time()
    (X, y), metadata = load_data(task_id=data_task_id)
    logger.info("Successfully loaded data from feature store in %.2f seconds.", time.time() - t1)

    logger.info("Loading model from model registry...")
    t1 = time.time()
    model = load_model_from_model_registry(task_id=training_task_id)
    logger.info("Successfully loaded model from model registry in %.2f seconds.", time.time() - t1)

    logger.info("Making predictions...")
    t1 = time.time()
    predictions = forecast(model, X, fh=fh)
    metadata["predictions_datetime_utc_start"] = (
        predictions.index.get_level_values(level="datetime_utc").min().strftime(metadata["datetime_format"])
    )
    metadata["predictions_datetime_utc_end"] = (
        predictions.index.get_level_values(level="datetime_utc").max().strftime(metadata["datetime_format"])
    )
    logger.info(
        "Forecasted energy consumption from %s to %s.",
        metadata["predictions_datetime_utc_start"],
        metadata["predictions_datetime_utc_end"],
    )
    logger.info("Successfully made predictions in %.2f seconds.", time.time() - t1)

    logger.info("Saving predictions...")
    t1 = time.time()
    ds = save(task, X, y, predictions, metadata)
    metadata["predictions_dataset_id"] = ds.id
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully saved predictions in %.2f seconds.", time.time() - t1)


def load_model_from_model_registry(task_id: str):
    """
    This function loads a model from the Model Registry.
    The model is downloaded, saved locally, and loaded into memory.
    """
    task_artifacts = get_task_artifacts(task_id=task_id)
    model = task_artifacts["best_forecaster"].get()

    # mr = project.get_model_registry()
    # model_registry_reference = mr.get_model(name="best_model", version=model_version)
    # model_dir = model_registry_reference.download()
    # model_path = Path(model_dir) / "best_model.pkl"

    # model = load_model(model_path)

    return model


def forecast(model, X: pd.DataFrame, fh: int = 24):
    """
    Get a forecast of the total load for the given areas and consumer types.

    Args:
        model (sklearn.base.BaseEstimator): Fitted model that implements the predict method.
        X (pd.DataFrame): Exogenous data with area, consumer_type, and datetime_utc as index.
        fh (int): Forecast horizon.

    Returns:
        pd.DataFrame: Forecast of total load for each area, consumer_type, and datetime_utc.
    """
    all_areas = X.index.get_level_values(level=0).unique()
    all_consumer_types = X.index.get_level_values(level=1).unique()
    latest_datetime = X.index.get_level_values(level=2).max()

    start = latest_datetime + 1
    end = start + fh - 1
    fh_range = pd.date_range(start=start.to_timestamp(), end=end.to_timestamp(), freq="H")
    fh_range = pd.PeriodIndex(fh_range, freq="H")

    index = pd.MultiIndex.from_product(
        [all_areas, all_consumer_types, fh_range],
        names=["area", "consumer_type", "datetime_utc"],
    )
    X_forecast = pd.DataFrame(index=index)
    X_forecast["area_exog"] = X_forecast.index.get_level_values(0)
    X_forecast["consumer_type_exog"] = X_forecast.index.get_level_values(1)

    predictions = model.predict(X=X_forecast)

    return predictions


def save(task, X: pd.DataFrame, y: pd.DataFrame, predictions: pd.DataFrame, metadata: dict):
    """Save the input data, target data, and predictions."""
    task.register_artifact("X", X)
    task.register_artifact("y", y)
    task.register_artifact("predictions", predictions)

    predictions.to_csv(PROCESSED_DIR / "predictions.csv", index_label=["area", "consumer_type", "datetime_utc"])
    save_json(metadata, PROCESSED_DIR / "batch_metadata.json")

    ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        dataset_version=metadata["feature_group_version"],
        # use_current_task=True,
        parent_datasets=[metadata["feature_store_id"]],
        dataset_tags=[metadata["predictions_datetime_utc_start"], metadata["predictions_datetime_utc_end"]],
    )

    ds.add_files(path=PROCESSED_DIR / "predictions.csv", verbose=True)
    ds.add_files(path=PROCESSED_DIR / "batch_metadata.json", verbose=True)

    ds.upload(verbose=True)
    ds.finalize()

    return ds
