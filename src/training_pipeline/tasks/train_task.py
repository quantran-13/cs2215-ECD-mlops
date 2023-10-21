import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from root import MODEL_DIR
from src.training_pipeline.src import train
from src.utils.logger import get_logger
from src.utils.task_utils import save_model

logger = get_logger("logs", __name__)


if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Training",
        task_type=TaskTypes.training,
        tags="training-pipeline",
    )

    args = {
        # "data_task_id": "OVERWRITE_ME",
        # "hpo_task_id": "OVERWRITE_ME",
        "forecasting_horizon": 24,
        "data_task_id": "a1cef1cc2ccb491e8e2601b4bd71195f",
        "hpo_task_id": "30dcb4682efe478cb37ea8b1e7d86f69",
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Starting training task...")
    t1 = time.time()
    # Train model.
    results = train.run_from_best_config(
        task, data_task_id=args["data_task_id"], hpo_task_id=args["hpo_task_id"], fh=args["forecasting_horizon"]
    )
    logger.info("Successfully ran hyperparameter tuning task in %.2f seconds.", time.time() - t1)

    t1 = time.time()
    logger.info("Uploading model and metadata...")
    save_model(results["model"], MODEL_DIR / "model.pkl")
    task.upload_artifact("model", MODEL_DIR / "model.pkl")
    task.upload_artifact("best_forecaster", results["model"])
    task.upload_artifact("metadata", results["metadata"])
    logger.info("Successfully uploaded model and metadata in %.2f seconds.", time.time() - t1)

    print("Done!")
