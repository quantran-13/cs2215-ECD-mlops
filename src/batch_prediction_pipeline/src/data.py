from typing import Any
import pandas as pd
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)


def load_data(task_id: str, target: str = "energy_consumption") -> tuple[tuple[pd.DataFrame, ...], dict[str, Any]]:
    """Load data for a given time range from the feature store.

    Args:
        fs: Feature store.
        feature_view_version: Feature view version.
        start_datetime: Start datetime.
        end_datetime: End datetime.
        target: Name of the target feature.

    Returns:
        Tuple of exogenous variables and the time series to be forecasted.
    """
    task_artifacts = get_task_artifacts(task_id=task_id)
    data: pd.DataFrame = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()
    logger.info(
        "Loading features from %s to %s...", metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]
    )

    X, y = prepare_data(data, target=target)

    for char in ["X", "y"]:
        arr = locals()[char]
        logger.info("%s shape: %s", char, arr.shape)
        logger.info("%s columns: %s", char, arr.columns.tolist())
        logger.info(
            "%s max datetime: %s", char, arr.index.get_level_values("datetime_utc").max().to_timestamp().isoformat()
        )
        logger.info(
            "%s min datetime: %s", char, arr.index.get_level_values("datetime_utc").min().to_timestamp().isoformat()
        )

    return (X, y), metadata


def prepare_data(data: pd.DataFrame, target: str = "energy_consumption") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Structure the data for training:
    - Set the index as is required by sktime.
    - Prepare exogenous variables.
    - Prepare the time series to be forecasted.
    - Split the data into train and test sets.
    """
    # Set the index as is required by sktime.
    data["datetime_utc"] = pd.PeriodIndex(data["datetime_utc"], freq="H")
    data = data.set_index(["area", "consumer_type", "datetime_utc"]).sort_index()

    # Prepare exogenous variables.
    X = data.drop(columns=[target])
    # Prepare the time series to be forecasted.
    y = data[[target]]

    return X, y
