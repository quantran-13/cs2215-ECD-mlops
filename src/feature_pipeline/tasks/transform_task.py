import argparse
import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.feature_pipeline.src import transform
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform task")
    parser.add_argument(
        "--artifacts_task_id", type=str, help="Artifacts task ID", default="68629139079948d197e1eebb8249c130"
    )
    args = parser.parse_args()
    print(f"Arguments: {args}")

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Transforming data",
        task_type=TaskTypes.data_processing,
    )
    task.add_tags(["feature-pipeline", "transform"])
    task.connect(args)
    # task.execute_remotely()

    logger.info("Transforming data.")
    task_artifacts = get_task_artifacts(task_id=args.artifacts_task_id)
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()

    t1 = time.time()
    data = transform.transform(data)
    logger.info("Successfully transformed data in %.2f seconds.", time.time() - t1)

    task.add_tags([metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]])

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
