import datetime as dt
import sys
import time
from pathlib import Path

from clearml import Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.feature_pipeline.src import extract
from src.utils.logger import get_logger
from src.utils.task_utils import get_task_artifacts

logger = get_logger("logs", __name__)

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Extracting data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )

    args = {
        # "artifacts_task_id": "OVERWRITE_ME",
        "artifacts_task_id": "3dfe30f7f8ca4619b535e43f64f66d05",
        "export_end_reference_datetime": "",
        "days_delay": 15,
        "days_export": 30,
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    logger.info("Extracting data from API.")

    task_artifacts = get_task_artifacts(task_id=args["artifacts_task_id"])
    data = task_artifacts["data"].get()

    export_end_reference_datetime = args["export_end_reference_datetime"]
    if export_end_reference_datetime == "":
        export_end_reference_datetime = None
    else:
        export_end_reference_datetime = dt.datetime.strptime(export_end_reference_datetime, "%Y-%m-%d %H:%M")

    days_delay = args["days_delay"]
    days_export = args["days_export"]

    t1 = time.time()
    data, metadata = extract.from_file(data, export_end_reference_datetime, days_delay, days_export)
    if metadata["num_unique_samples_per_time_series"] < days_export * 24:
        raise RuntimeError(
            f"Could not extract the expected number of samples from the api: {metadata['num_unique_samples_per_time_series']} < {days_export * 24}. \
            Check out the API at: https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour "
        )
    metadata["raw_feature_store_id"] = task_artifacts["feature_store"].get()
    logger.info("Successfully extracted data in %.2f seconds.", time.time() - t1)

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    print("Done!")
