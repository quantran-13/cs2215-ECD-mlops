import datetime as dt
import sys
from pathlib import Path

from clearml.automation import PipelineController

CURRENT_DIR = Path(__file__).parent.parent.parent
print(CURRENT_DIR)
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.utils.logger import get_logger

logger = get_logger(__name__)


if __name__ == "__main__":
    pipe = PipelineController(name="ETL", project=PROJECT_NAME, version="0.0.1", add_pipeline_tags=True)
    pipe.set_default_execution_queue("default")

    pipe.add_parameter(
        "run_datetime",
        dt.datetime(2023, 6, 30, 21),
    )
    pipe.add_parameter(
        "days_delay",
        15,
    )
    pipe.add_parameter(
        "days_export",
        90,
    )
    pipe.add_parameter("feature_group_version", "1.0.0")

    pipe.add_step(
        name="extract_data",
        base_task_name="Extracting data",
        base_task_project=PROJECT_NAME,
        parameter_override={
            "General/export_end_reference_datetime": "${pipeline.run_datetime}",
            "General/days_delay": "${pipeline.days_delay}",
            "General/days_export": "${pipeline.days_export}",
        },
    )
    pipe.add_step(
        name="transform_data",
        base_task_name="Transforming data",
        base_task_project=PROJECT_NAME,
        parents=["extract_data"],
        parameter_override={
            "General/artifacts_task_id": "${pipeline.extract_data.id}",
        },
    )
    pipe.add_step(
        name="validate_data",
        base_task_name="Validating data",
        base_task_project=PROJECT_NAME,
        parents=["transform_data"],
        parameter_override={
            "General/artifacts_task_id": "${pipeline.transform_data.id}",
        },
    )
    pipe.add_step(
        name="load_data",
        base_task_name="Loading data",
        base_task_project=PROJECT_NAME,
        parents=["validate_data"],
        parameter_override={
            "General/artifacts_task_id": "${pipeline.validate_data.id}",
            "General/feature_group_version": "${pipeline.feature_group_version}",
        },
    )

    # For debugging purposes use local jobs
    pipe.start_locally()

    # Starting the pipeline (in the background)
    # pipe.start()

    print("Done")
