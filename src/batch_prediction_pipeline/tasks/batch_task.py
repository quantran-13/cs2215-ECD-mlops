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
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Batch Prediction",
        task_type=TaskTypes.testing,
        tags="batch-prediction-pipeline",
    )

    args = {
        # "data_task_id": "OVERWRITE_ME",
        # "training_task_id": "OVERWRITE_ME",
        "forecasting_horizon": 24,
        "data_task_id": "a1cef1cc2ccb491e8e2601b4bd71195f",
        "training_task_id": "df898d2e83374edf828ae469c46d47cc",
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Starting batch prediction task...")
    t1 = time.time()
    # Batch prediction.
    data_task_id = args["data_task_id"]
    training_task_id = args["training_task_id"]
    fh = args["forecasting_horizon"]
    batch.predict(task, data_task_id=data_task_id, training_task_id=training_task_id, fh=fh)
    logger.info("Successfully ran batch prediction task in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
