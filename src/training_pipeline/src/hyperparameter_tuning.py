import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

import os
import time

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from root import MODEL_DIR, OUTPUT_DIR
from sktime.forecasting.model_selection import ExpandingWindowSplitter, ForecastingGridSearchCV
from sktime.performance_metrics.forecasting import MeanAbsolutePercentageError
from sktime.utils.plotting import plot_windows
from src.training_pipeline.configs import search as search_configs
from src.training_pipeline.src.data import load_dataset
from src.training_pipeline.src.models import build_model
from src.utils.logger import get_logger
from src.utils.task_utils import save_model

logger = get_logger("logs", __name__)


def run(task, task_id: str, model_cfg: dict, fh: int = 24, k: int = 3):
    """Run hyperparameter optimization search.

    Args:
        fh (int, optional): Forecasting horizon. Defaults to 24.
        feature_view_version (Optional[int], optional): feature store - feature view version.
             If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.
        training_dataset_version (Optional[int], optional): feature store - feature view - training dataset version.
            If none, it will try to load the version from the cached feature_view_metadata.json file. Defaults to None.
    """
    task_logger = task.get_logger()

    (y_train, y_test, X_train, X_test), metadata = load_dataset(task_id=task_id, fh=fh)
    task.register_artifact("y_train", y_train)
    task.register_artifact("y_test", y_test)
    task.register_artifact("X_train", X_train)
    task.register_artifact("X_test", X_test)

    results = run_hyperparameter_optimization(y_train, X_train, model_cfg, task_logger, fh=fh, k=k)
    hpo_result = results.cv_results_.sort_values("rank_test_MeanAbsolutePercentageError")
    hpo_result = hpo_result.rename(
        columns={
            "mean_test_MeanAbsolutePercentageError": "mean_MAPE",
            "mean_fit_time": "mean_fit_time",
            "mean_pred_time": "mean_prediction_time",
            "rank_test_MeanAbsolutePercentageError": "rank",
        }
    )
    task_logger.report_table(title="HPO results", series="HPO results", iteration=0, table_plot=hpo_result)
    task.register_artifact("cv_results", hpo_result)
    task.upload_artifact("best_params", results.best_params_)
    task.upload_artifact("best_score", results.best_score_)

    logger.info("Best params: %s", results.best_params_)
    logger.info("Best index: %s", results.best_index_)
    logger.info("Best score: %s", results.best_score_)
    logger.info("Mean fit time in %.2f seconds.", hpo_result["mean_fit_time"].mean())
    logger.info("Mean prediction time in %.2f seconds.", hpo_result["mean_prediction_time"].mean())

    # Save best model.
    logger.info("Saving best model...")
    t1 = time.time()
    save_model(results.best_forecaster_, MODEL_DIR / "model.pkl")
    task.upload_artifact("model", MODEL_DIR / "model.pkl")
    task.upload_artifact("best_forecaster", results.best_forecaster_)
    task.upload_artifact("metadata", metadata)
    task.add_tags([metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]])
    logger.info("Successfully saved best model in %.2f seconds.", time.time() - t1)


def run_hyperparameter_optimization(
    y_train: pd.DataFrame, X_train: pd.DataFrame, model_cfg: dict, task_logger, fh: int = 24, k: int = 3
):
    """Run hyperparameter optimization search."""
    model = build_model(model_cfg)
    data_length = len(y_train.index.get_level_values(-1).unique())
    assert data_length >= fh * 10, "Not enough data to perform a 3 fold CV."

    cv_step_length = data_length // k
    initial_window = max(fh * 3, cv_step_length - fh)
    cv = ExpandingWindowSplitter(
        step_length=cv_step_length,
        fh=np.arange(fh) + 1,
        initial_window=initial_window,
    )
    render_cv_scheme(cv, y_train, task_logger)

    # search = ForecastingSkoptSearchCV(
    #     forecaster=model,
    #     cv=cv,
    #     param_distributions=search_configs.search_spaces,
    #     n_iter=2,
    #     random_state=42,
    #     scoring=MeanAbsolutePercentageError(symmetric=False),
    #     strategy="refit",
    #     n_jobs=-1,
    #     refit=True,
    #     verbose=10,
    #     error_score="raise",
    # )
    search = ForecastingGridSearchCV(
        forecaster=model,
        cv=cv,
        param_grid=search_configs.search_spaces,
        scoring=MeanAbsolutePercentageError(symmetric=False),
        strategy="refit",
        n_jobs=-1,
        refit=True,
        verbose=10,
        # return_train_score=True,
        error_score="raise",
    )
    result = search.fit(y=y_train, X=X_train, fh=np.arange(fh) + 1)

    return result


def render_cv_scheme(cv, y_train: pd.DataFrame, task_logger, delete_from_disk: bool = True):
    """Render the CV scheme used for training and log it."""
    random_time_series = y_train.groupby(level=[0, 1]).get_group((1, 111)).reset_index(level=[0, 1], drop=True)
    plot_windows(cv, random_time_series)

    save_path = str(OUTPUT_DIR / "cv_scheme.png")
    plt.savefig(save_path)

    task_logger.report_image(title="CV scheme", series="CV scheme", iteration=0, local_path=save_path)

    if delete_from_disk:
        os.remove(save_path)
