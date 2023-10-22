import ast
import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.training_pipeline.src import hyperparameter_tuning as hpo
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Hyperparameter Tuning",
        task_type=TaskTypes.training,
        tags="training-pipeline",
    )

    args = {
        # "artifacts_task_id": "OVERWRITE_ME",
        "artifacts_task_id": "8568e970ffd440ad9070de0f314f37b7",
        "forecasting_horizon": 24,
        "k": 3,
        "lag_feature_lag_min": 1,
        "lag_feature_lag_max": 72,
        "lag_feature_mean": [[1, 24], [1, 48], [1, 72]],
        "lag_feature_std": [[1, 24], [1, 48], [1, 72]],
        "datetime_features": ["day_of_week", "hour_of_day"],
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Starting hyperparameter tuning task...")
    t1 = time.time()
    lag: list[int] = list(range(args["lag_feature_lag_min"], args["lag_feature_lag_max"] + 1))
    mean: list[list[int]] = (
        ast.literal_eval(args["lag_feature_mean"])
        if isinstance(args["lag_feature_mean"], str)
        else args["lag_feature_mean"]
    )
    std: list[list[int]] = (
        ast.literal_eval(args["lag_feature_std"])
        if isinstance(args["lag_feature_std"], str)
        else args["lag_feature_std"]
    )
    dt_features: list[str] = (
        ast.literal_eval(args["datetime_features"])
        if isinstance(args["datetime_features"], str)
        else args["datetime_features"]
    )
    model_cfg = {
        "forecaster_transformers__window_summarizer__lag_feature__lag": lag,
        "forecaster_transformers__window_summarizer__lag_feature__mean": mean,
        "forecaster_transformers__window_summarizer__lag_feature__std": std,
        "daily_season__manual_selection": dt_features,
    }
    # Save model config to artifacts
    task.upload_artifact("model_cfg", model_cfg)

    # Hyperparameter optimization
    hpo.run(task, task_id=args["artifacts_task_id"], model_cfg=model_cfg, fh=args["forecasting_horizon"], k=args["k"])
    logger.info("Successfully ran hyperparameter tuning task in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
