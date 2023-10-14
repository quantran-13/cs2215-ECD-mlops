import pandas as pd
from clearml import Dataset

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR
from src.utils.task_utils import save_json


def to_feature_store(data: pd.DataFrame, metadata: dict):
    data.to_csv(PROCESSED_DIR / "processed.csv", index_label=["HourUTC"])
    save_json(metadata, PROCESSED_DIR / "metadata.json")

    ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        dataset_version=metadata["feature_group_version"],
        use_current_task=True,
        description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
    )
    ds.add_files(path=PROCESSED_DIR / "processed.csv", verbose=True)
    ds.add_files(path=PROCESSED_DIR / "metadata.json", verbose=True)

    ds.upload(verbose=True)
