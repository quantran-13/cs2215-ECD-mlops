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

    pipe.add_parameter("run_datetime", "2023-06-30 21:00", "The value must be in the format: YYYY-MM-DD HH:MM")
    pipe.add_parameter("days_delay", 15)
    pipe.add_parameter("days_export", 90)
    pipe.add_parameter("feature_group_version", "1.0.0")
    pipe.add_parameter("lag_time", "1, 2, 3, 24, 168, 720", "The value must be a comma-separated list of integers.")
    pipe.add_parameter("warn_on_na", True)
    pipe.add_parameter("drop_na", False)

    params = pipe.get_parameters()
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
            "General/artifacts_task_id": "${extract_data.id}",
        },
    )
    pipe.add_step(
        name="validate_data",
        base_task_name="Validating data",
        base_task_project=PROJECT_NAME,
        parents=["transform_data"],
        parameter_override={
            "General/artifacts_task_id": "${transform_data.id}",
        },
    )
    pipe.add_step(
        name="load_data",
        base_task_name="Loading data",
        base_task_project=PROJECT_NAME,
        parents=["validate_data"],
        parameter_override={
            "General/artifacts_task_id": "${validate_data.id}",
            "General/feature_group_version": "${pipeline.feature_group_version}",
        },
    )
    lag_time = params["lag_time"].split(",")
    pipe.add_step(
        name="create_feature",
        base_task_name="Creating features",
        base_task_project=PROJECT_NAME,
        parents=["load_data"],
        parameter_override={
            "General/artifacts_task_id": "${load_data.id}",
            "General/lag_time": lag_time,
            "General/warn_on_na": "${pipeline.warn_on_na}",
            "General/drop_na": "${pipeline.drop_na}",
        },
    )

    # For debugging purposes use local jobs
    pipe.start_locally()

    # Starting the pipeline (in the background)
    # pipe.start()

    print("Done")
