import pandas as pd
from clearml import Dataset
from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR
from src.utils.logger import get_logger
from src.utils.task_utils import save_json

logger = get_logger("logs", __name__)


def to_feature_store(data: pd.DataFrame, metadata: dict, parent_datasets: list[str] | None = None):
    data.to_csv(PROCESSED_DIR / "processed.csv", index_label=["index"])
    save_json(metadata, PROCESSED_DIR / "metadata.json")

    ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        dataset_version=metadata["feature_group_version"],
        # use_current_task=True,
        parent_datasets=parent_datasets,
        description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
        dataset_tags=[metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]],
    )
    ds.add_files(path=PROCESSED_DIR / "processed.csv", verbose=True)
    ds.add_files(path=PROCESSED_DIR / "metadata.json", verbose=True)

    ds.upload(verbose=True)
    ds.finalize()

    return ds
