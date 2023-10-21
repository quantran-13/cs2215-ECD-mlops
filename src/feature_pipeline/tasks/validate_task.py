import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.feature_pipeline.src import validate
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Validating data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )

    args = {
        # "artifacts_task_id": "OVERWRITE_ME",
        "artifacts_task_id": "09a03899dcc541268b6371a69e6da269",
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Validating data.")

    task_artifacts = get_task_artifacts(task_id=args["artifacts_task_id"])
    data = task_artifacts["data"].get()
    metadata = task_artifacts["metadata"].get()

    t1 = time.time()
    validation_expectation_suite = validate.build_expectation_suite(data)
    logger.info("Successfully built validation expectation suite in %.2f seconds.", time.time() - t1)
    result = validate.validate(data, validation_expectation_suite)
    logger.info("Successfully validated data in %.2f seconds.", time.time() - t1)

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    task.upload_artifact("validation_expectation_suite", validation_expectation_suite)
    task.upload_artifact("validation_result", result)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    print("Done!")
