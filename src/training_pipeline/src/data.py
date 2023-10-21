import pandas as pd
from typing import Any
from sktime.forecasting.model_selection import temporal_train_test_split
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)


def load_dataset(task_id: str, fh: int = 24):
    """Load features from feature store.

    Args:
        training_dataset_version (int): Feature store training dataset version to load data from.
        fh (int, optional): Forecast horizon. Defaults to 24.

    Returns:
        Train and test splits loaded from the feature store as pandas dataframes.
    """
    task_artifacts = get_task_artifacts(task_id=task_id)
    data: pd.DataFrame = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()

    y_train, y_test, X_train, X_test = prepare_data(data, fh=fh)

    exp: dict[str, Any] = {"fh": fh}
    for split in ["train", "test"]:
        split_X = locals()[f"X_{split}"]
        split_y = locals()[f"y_{split}"]

        exp[f"{split}_split_metadata"] = {
            "timespan": [
                split_X.index.get_level_values("datetime_utc").min().to_timestamp().isoformat(),
                split_X.index.get_level_values("datetime_utc").max().to_timestamp().isoformat(),
            ],
            "dataset_size": len(split_X),
            "num_areas": len(split_X.index.get_level_values(0).unique()),
            "num_consumer_types": len(split_X.index.get_level_values(1).unique()),
            "y_features": split_y.columns.tolist(),
            "X_features": split_X.columns.tolist(),
        }

    metadata["experiment"] = exp

    return (y_train, y_test, X_train, X_test), metadata


def prepare_data(
    data: pd.DataFrame, target: str = "energy_consumption", fh: int = 24
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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

    y_train, y_test, X_train, X_test = temporal_train_test_split(y, X, test_size=fh)

    for split in ["train", "test"]:
        for char in ["X", "y"]:
            arr = locals()[f"{char}_{split}"]
            logger.info("%s_%s shape: %s", char, split, arr.shape)
            logger.info("%s_%s columns: %s", char, split, arr.columns.tolist())
            logger.info(
                "%s_%s max datetime: %s",
                char,
                split,
                arr.index.get_level_values("datetime_utc").max().to_timestamp().isoformat(),
            )
            logger.info(
                "%s_%s min datetime: %s",
                char,
                split,
                arr.index.get_level_values("datetime_utc").min().to_timestamp().isoformat(),
            )

    return y_train, y_test, X_train, X_test
