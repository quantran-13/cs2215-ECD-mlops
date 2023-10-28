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

logger = get_logger("logs", __name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load task")
    parser.add_argument(
        "--artifacts-task-id", type=str, help="Artifacts task ID", default="805c0653252a4e5594c916835abaccec"
    )
    parser.add_argument("--fg-ver", type=int, help="Feature group version", default=1)
    args = parser.parse_args()
    print(f"Arguments: {args}")

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Loading data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )
    task.connect(args)

    logger.info("Loading data to the feature store.")
    task_id = args.artifacts_task_id
    task_artifacts = get_task_artifacts(task_id=task_id)
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()
    metadata["feature_group_version"] = args.fg_ver

    t1 = time.time()
    parent_datasets_id = metadata["feature_store_id"]
    ds, metadata = load.to_feature_store(data, metadata, parent_datasets_id=parent_datasets_id)
    logger.info("Successfully loaded data to the feature store in %.2f seconds.", time.time() - t1)

    task.add_tags([metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]])

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
