import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes
from src.data import create_feature

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.feature_pipeline.src import load
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger(__name__)

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Creating features",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )

    args = {
        "artifacts_task_id": "OVERWRITE_ME",
        "lag_time": (1, 2, 3, 24, 168, 720),
        "warn_on_na": True,
        "drop_na": False,
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Starting to create features ...")

    task_id = args["artifacts_task_id"]
    lag_time = args["lag_time"]
    warn_on_na = args["warn_on_na"]
    drop_na = args["drop_na"]

    task_artifacts = get_task_artifacts(task_id=task_id)
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()
    metadata["lag_time"] = lag_time
    metadata["warn_on_na"] = warn_on_na
    metadata["drop_na"] = drop_na

    t1 = time.time()
    data = create_feature.create_feature(data, lag_time, warn_on_na, drop_na)
    logger.info("Successfully created features in %.2f seconds.", time.time() - t1)
    ds = load.to_feature_store(data, metadata, [task_artifacts["feature_store"].get()])
    logger.info("Successfully loaded data to the feature store in %.2f seconds.", time.time() - t1)

    t1 = time.time()
    task.upload_artifact("feature_store", ds.id)
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
