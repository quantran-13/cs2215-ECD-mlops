import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

import os
import time
from collections import OrderedDict
from typing import OrderedDict as OrderedDictType  # noqa

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from root import MODEL_DIR, OUTPUT_DIR
from sktime.performance_metrics.forecasting import mean_absolute_percentage_error, mean_squared_percentage_error
from sktime.utils.plotting import plot_series
from src.training_pipeline.src.data import load_dataset
from src.training_pipeline.src.models import build_baseline_model, build_model
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts, save_model

logger = get_logger("logs", __name__)


def run_from_best_config(task, data_task_id: str, hpo_task_id: str, fh: int = 24):
    """Train and evaluate on the test set the best model found in the hyperparameter optimization run.
    # After training and evaluating it uploads the artifacts to wandb & hopsworks model registries.

    Args:
        fh (int, optional): Forecasting horizon. Defaults to 24.
        feature_view_version (Optional[int], optional): feature store - feature view version.
             If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.
        training_dataset_version (Optional[int], optional): feature store - feature view - training dataset version.
            If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.

    Returns:
        dict: Dictionary containing metadata about the training experiment.
    """
    # Get task logger.
    task_logger = task.get_logger()

    # Load data.
    logger.info("Loading data from feature store...")
    t1 = time.time()
    (y_train, y_test, X_train, X_test), metadata = load_dataset(task_id=data_task_id, fh=fh)
    task.register_artifact("y_train", y_train)
    task.register_artifact("y_test", y_test)
    task.register_artifact("X_train", X_train)
    task.register_artifact("X_test", X_test)
    logger.info("Successfully loaded data from feature store in %.2f seconds.", time.time() - t1)

    # Load best model configuration.
    logger.info("Loading best model configuration...")
    t1 = time.time()
    task_artifacts = get_task_artifacts(task_id=hpo_task_id)
    best_config_artifact = task_artifacts["best_params"].get()
    best_config_artifact.update(task_artifacts["model_cfg"].get())

    task.upload_artifact("model_cfg", best_config_artifact)
    logger.info("Successfully loaded best model configuration in %.2f seconds.", time.time() - t1)

    # Baseline model
    logger.info("Building & training baseline model...")
    t1 = time.time()
    baseline_forecaster = build_baseline_model(seasonal_periodicity=fh)
    baseline_forecaster = train_model(baseline_forecaster, y_train, X_train, fh=fh)
    _, metrics_baseline = evaluate(baseline_forecaster, y_test, X_test)
    slices = metrics_baseline.pop("slices")
    for k, v in metrics_baseline.items():
        logger.info("Baseline test %s: %s", k, v)
    task_logger.report_scalar("Test MAPE", "Baseline", metrics_baseline["MAPE"], iteration=0)
    task_logger.report_scalar("Test RMSPE", "Baseline", metrics_baseline["RMSPE"], iteration=0)
    task_logger.report_table(title="Slices results", series="Baseline results", iteration=0, table_plot=slices)
    logger.info("Successfully built & trained baseline model in %.2f seconds.", time.time() - t1)

    # Build & train best model
    logger.info("Building & training best model...")
    t1 = time.time()
    best_model = build_model(best_config_artifact)
    best_forecaster = train_model(best_model, y_train, X_train, fh=fh)
    y_pred, metrics = evaluate(best_forecaster, y_test, X_test)
    slices = metrics.pop("slices")
    for k, v in metrics.items():
        logger.info("Best model test %s: %s", k, v)
    task_logger.report_scalar("Test MAPE", "Best model", metrics["MAPE"], iteration=0)
    task_logger.report_scalar("Test RMSPE", "Best model", metrics["RMSPE"], iteration=0)
    task_logger.report_table(title="Slices results", series="Best model results", iteration=0, table_plot=slices)
    logger.info("Successfully built & trained best model in %.2f seconds.", time.time() - t1)

    # Render best model on the test set.
    logger.info("Rendering best model on the test set...")
    t1 = time.time()
    results = OrderedDict({"y_train": y_train, "y_test": y_test, "y_pred": y_pred})
    render(results, task_logger, prefix="images_test")
    logger.info("Successfully rendered best model on the test set in %.2f seconds.", time.time() - t1)

    # Update best model with the test set.
    # NOTE: Method update() is not supported by LightGBM + Sktime. Instead we will retrain the model on the entire dataset.
    # best_forecaster = best_forecaster.update(y_test, X=X_test)
    logger.info("Retraining best model on the entire dataset and forecasting...")
    t1 = time.time()
    best_forecaster = train_model(
        model=best_forecaster,
        y_train=pd.concat([y_train, y_test]).sort_index(),
        X_train=pd.concat([X_train, X_test]).sort_index(),
        fh=fh,
    )
    X_forecast = compute_forecast_exogenous_variables(X_test, fh)
    y_forecast = forecast(best_forecaster, X_forecast)
    logger.info(
        "Forecasting from %s to %s.",
        y_forecast.index.get_level_values("datetime_utc").min().to_timestamp().isoformat(),
        y_forecast.index.get_level_values("datetime_utc").max().to_timestamp().isoformat(),
    )
    logger.info(
        "Successfully retrained best model on the entire dataset and forecasted in %.2f seconds.", time.time() - t1
    )

    # Render best model future forecasts.
    logger.info("Rendering best model future forecasts...")
    t1 = time.time()
    results = OrderedDict(
        {
            "y_train": y_train,
            "y_test": y_test,
            "y_forecast": y_forecast,
        }
    )
    render(results, task_logger, prefix="images_forecast")
    logger.info("Successfully rendered best model future forecasts in %.2f seconds.", time.time() - t1)

    # Save best model.
    logger.info("Saving best model...")
    t1 = time.time()
    # metadata = {"model_version": model_version}
    save_model(best_forecaster, MODEL_DIR / "model.pkl")
    task.upload_artifact("model", MODEL_DIR / "model.pkl")
    task.upload_artifact("best_forecaster", best_forecaster)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully saved best model in %.2f seconds.", time.time() - t1)


def train_model(model, y_train: pd.DataFrame, X_train: pd.DataFrame, fh: int):
    """Train the forecaster on the given training set and forecast horizon."""
    fh_ = np.arange(fh) + 1
    model.fit(y_train, X=X_train, fh=fh_)

    return model


def evaluate(forecaster, y_test: pd.DataFrame, X_test: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Evaluate the forecaster on the test set by computing the following metrics:
        - RMSPE
        - MAPE
        - Slices: RMSPE, MAPE

    Args:
        forecaster: model following the sklearn API
        y_test (pd.DataFrame): time series to forecast
        X_test (pd.DataFrame): exogenous variables

    Returns:
        The predictions as a pd.DataFrame and a dict of metrics.
    """
    y_pred = forecaster.predict(X=X_test)

    # Compute aggregated metrics.
    results = dict()
    rmspe = mean_squared_percentage_error(y_test, y_pred, squared=False)
    results["RMSPE"] = rmspe
    mape = mean_absolute_percentage_error(y_test, y_pred, symmetric=False)
    results["MAPE"] = mape

    # Compute metrics per slice.
    y_test_slices = y_test.groupby(["area", "consumer_type"])
    y_pred_slices = y_pred.groupby(["area", "consumer_type"])
    slices = pd.DataFrame(columns=["area", "consumer_type", "RMSPE", "MAPE"])
    for y_test_slice, y_pred_slice in zip(y_test_slices, y_pred_slices):
        (area_y_test, consumer_type_y_test), y_test_slice_data = y_test_slice
        (area_y_pred, consumer_type_y_pred), y_pred_slice_data = y_pred_slice

        assert area_y_test == area_y_pred and consumer_type_y_test == consumer_type_y_pred, "Slices are not aligned."

        rmspe_slice = mean_squared_percentage_error(y_test_slice_data, y_pred_slice_data, squared=False)
        mape_slice = mean_absolute_percentage_error(y_test_slice_data, y_pred_slice_data, symmetric=False)

        slice_results = pd.DataFrame(
            {
                "area": [area_y_test],
                "consumer_type": [consumer_type_y_test],
                "RMSPE": [rmspe_slice],
                "MAPE": [mape_slice],
            }
        )
        slices = pd.concat([slices, slice_results], ignore_index=True)

    results["slices"] = slices

    return y_pred, results


def render(
    timeseries: OrderedDictType[str, pd.DataFrame],
    task_logger,
    prefix: str | None = None,
    delete_from_disk: bool = True,
):
    """Render the timeseries as a single plot per (area, consumer_type)."""
    grouped_timeseries = OrderedDict()
    for split, df in timeseries.items():
        df = df.reset_index(level=[0, 1])
        groups = df.groupby(["area", "consumer_type"])
        for group_name, split_group_values in groups:
            group_values = grouped_timeseries.get(group_name, {})

            grouped_timeseries[group_name] = {
                f"{split}": split_group_values["energy_consumption"],
                **group_values,
            }

    output_dir = OUTPUT_DIR / prefix if prefix else OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    for group_name, group_values_dict in grouped_timeseries.items():
        fig, ax = plot_series(*group_values_dict.values(), labels=group_values_dict.keys())
        fig.suptitle(f"Area: {group_name[0]} - Consumer type: {group_name[1]}")

        image_save_path = str(output_dir / f"{group_name[0]}_{group_name[1]}.png")
        plt.savefig(image_save_path)
        plt.close(fig)

        if prefix:
            task_logger.report_image(
                title=f"{prefix}",
                series=f"Area: {group_name[0]} - Consumer type: {group_name[1]}",
                iteration=0,
                local_path=image_save_path,
            )
        else:
            task_logger.report_image(
                title="Figure",
                series=f"Area: {group_name[0]} - Consumer type: {group_name[1]}",
                iteration=0,
                local_path=image_save_path,
            )

        if delete_from_disk:
            os.remove(image_save_path)


def compute_forecast_exogenous_variables(X_test: pd.DataFrame, fh: int):
    """Computes the exogenous variables for the forecast horizon."""
    X_forecast = X_test.copy()
    X_forecast.index = X_forecast.index.set_levels(X_forecast.index.levels[-1] + fh, level=-1)

    return X_forecast


def forecast(forecaster, X_forecast: pd.DataFrame):
    """Forecast the energy consumption for the given exogenous variables and time horizon."""
    return forecaster.predict(X=X_forecast)
