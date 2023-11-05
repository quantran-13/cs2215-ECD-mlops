import argparse
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

logger = get_logger(logdir="logs", name=__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load task")
    parser.add_argument("--artifacts_task_id", type=str, help="Artifacts task ID", required=True)
    parser.add_argument("--feature_group_version", type=str, help="Feature group version", default="1.0")
    args = parser.parse_args()
    print(f"Arguments: {args}")

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Loading data",
        task_type=TaskTypes.data_processing,
    )
    task.add_tags(["feature-pipeline", "load"])
    task.connect(args)

    logger.info("Loading data to the feature store.")
    task_id = args.artifacts_task_id
    task_artifacts = get_task_artifacts(task_id=task_id)
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()
    metadata["feature_group_version"] = args.feature_group_version

    t1 = time.time()
    parent_datasets_id = metadata["feature_store_id"]
    ds, metadata = load.to_feature_store(data=data, metadata=metadata, parent_datasets_id=parent_datasets_id)
    logger.info("Successfully loaded data to the feature store in %.2f seconds.", time.time() - t1)

    task.add_tags([metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]])

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
