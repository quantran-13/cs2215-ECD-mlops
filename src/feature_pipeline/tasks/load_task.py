import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.feature_pipeline.src import load
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Loading data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )

    args = {
        # "artifacts_task_id": "OVERWRITE_ME",
        # "feature_group_version": "OVERWRITE_ME",
        "artifacts_task_id": "95a37c105d724133bc38335e263648a8",
        "feature_group_version": 1,
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Loading data to the feature store.")

    task_artifacts = get_task_artifacts(task_id=args["artifacts_task_id"])
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()
    metadata["feature_group_version"] = args["feature_group_version"]

    t1 = time.time()
    ds = load.to_feature_store(data, metadata, parent_datasets=[metadata["raw_feature_store_id"]])
    logger.info("Successfully loaded data to the feature store in %.2f seconds.", time.time() - t1)

    t1 = time.time()
    metadata["feature_store_id"] = ds.id
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    print("Done!")
