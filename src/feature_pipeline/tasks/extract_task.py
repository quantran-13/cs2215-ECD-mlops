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
        # "feature_store_id": "OVERWRITE_ME",
        "export_end_reference_datetime": "2023-04-01 00:00",
        "days_delay": 15,
        "days_export": 30,
        "artifacts_task_id": "3dfe30f7f8ca4619b535e43f64f66d05",
        "feature_store_id": "649430da2e0247db8ef3a073e30223b2",
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
    # metadata["feature_store_id"] = task_artifacts["feature_store"].get()
    metadata["feature_store_id"] = args["feature_store_id"]
    logger.info("Successfully extracted data in %.2f seconds.", time.time() - t1)

    task.add_tags([metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]])

    t1 = time.time()
    task.upload_artifact("data", data)
    task.upload_artifact("metadata", metadata)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    logger.info("=" * 80)
    print("Done!")
