import argparse
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
    parser = argparse.ArgumentParser(description="Hyperparameter tuning task")
    parser.add_argument(
        "--artifacts_task_id", type=str, help="Artifacts task ID", default="8568e970ffd440ad9070de0f314f37b7"
    )
    parser.add_argument("--forecasting_horizon", type=int, help="Forecasting horizon", default=24)
    parser.add_argument("--k", type=int, help="K value", default=3)
    parser.add_argument("--lag_feature_lag_min", type=int, help="Lag feature lag min", default=1)
    parser.add_argument("--lag_feature_lag_max", type=int, help="Lag feature lag max", default=72)
    parser.add_argument("--lag_feature_mean", type=list, nargs="+", help="Lag feature mean", default=None)
    parser.add_argument("--lag_feature_std", type=list, nargs="+", help="Lag feature std", default=None)
    parser.add_argument("--datetime_features", type=list, nargs="+", help="Datetime features", default=None)
    args = parser.parse_args()
    if args.lag_feature_mean is None:
        args.lag_feature_mean = [[1, 24], [1, 48], [1, 72]]

    if args.lag_feature_std is None:
        args.lag_feature_std = [[1, 24], [1, 48], [1, 72]]

    if args.datetime_features is None:
        args.datetime_features = ["day_of_week", "hour_of_day"]
    print(f"Arguments: {args}")

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Hyperparameter Tuning",
        task_type=TaskTypes.training,
    )
    task.add_tags(["training-pipeline", "hyperparameter-tuning"])
    task.connect(args)
    # task.execute_remotely()

    logger.info("Starting hyperparameter tuning task...")
    t1 = time.time()
    lag: list[int] = list(range(args.lag_feature_lag_min, args.lag_feature_lag_max + 1))
    mean: list[list[int]] = (
        ast.literal_eval(args.lag_feature_mean) if isinstance(args.lag_feature_mean, str) else args.lag_feature_mean
    )
    std: list[list[int]] = (
        ast.literal_eval(args.lag_feature_std) if isinstance(args.lag_feature_std, str) else args.lag_feature_std
    )
    dt_features: list[str] = (
        ast.literal_eval(args.datetime_features) if isinstance(args.datetime_features, str) else args.datetime_features
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
    hpo.run(task, task_id=args.artifacts_task_id, model_cfg=model_cfg, fh=args.forecasting_horizon, k=args.k)
    logger.info("Successfully ran hyperparameter tuning task in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
