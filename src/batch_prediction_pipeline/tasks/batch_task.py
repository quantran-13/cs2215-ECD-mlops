import argparse
import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.batch_prediction_pipeline.src import batch
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch prediction task")
    parser.add_argument("--data_task_id", type=str, help="Data task ID", default="8568e970ffd440ad9070de0f314f37b7")
    parser.add_argument(
        "--training_task_id", type=str, help="Training task ID", default="fcafa4c31b3c4b31bb184594b5b63cea"
    )
    parser.add_argument("--forecasting_horizon", type=int, help="Forecasting horizon", default=24)
    args = parser.parse_args()
    print(f"Arguments: {args}")

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Batch Prediction",
        task_type=TaskTypes.testing,
    )
    task.add_tags(["batch-prediction-pipeline", "batch-prediction"])
    task.connect(args)
    # task.execute_remotely()

    logger.info("Starting batch prediction task...")
    t1 = time.time()
    # Batch prediction.
    data_task_id = args.data_task_id
    training_task_id = args.training_task_id
    fh = args.forecasting_horizon
    batch.predict(task, data_task_id=data_task_id, training_task_id=training_task_id, fh=fh)
    logger.info("Successfully ran batch prediction task in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
