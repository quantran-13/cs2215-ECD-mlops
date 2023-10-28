import pandas as pd
from clearml import Dataset
from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR
from src.utils.logger import get_logger
from src.utils.task_utils import save_json

logger = get_logger("logs", __name__)


def to_feature_store(data: pd.DataFrame, metadata: dict, parent_datasets_id: str):
    ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        dataset_version=metadata["feature_group_version"],
        # use_current_task=True,
        parent_datasets=[parent_datasets_id],
        description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
        dataset_tags=["storage", metadata["export_datetime_utc_start"], metadata["export_datetime_utc_end"]],
    )

    parent_dataset = Dataset.get(parent_datasets_id)
    local_path = parent_dataset.get_local_copy()

    if "processed.csv" in parent_dataset.list_files():
        df = pd.read_csv(f"{local_path}/processed.csv")
        df.drop(columns=["index"], inplace=True)
        df["datetime_utc"] = pd.to_datetime(df["datetime_utc"])

        data_ = pd.concat([df, data], ignore_index=True)
        data_.sort_values(by="datetime_utc", inplace=True, ascending=False)
        data_.drop_duplicates(keep="first", inplace=True)

        data_.to_csv(PROCESSED_DIR / "processed.csv", index_label=["index"])
    else:
        data_ = data.copy()
        data_.to_csv(PROCESSED_DIR / "processed.csv", index_label=["index"])

    ds.add_files(path=PROCESSED_DIR / "processed.csv", verbose=True)

    metadata["feature_store_id"] = ds.id
    save_json(metadata, PROCESSED_DIR / "metadata.json")
    ds.add_files(path=PROCESSED_DIR / "metadata.json", verbose=True)

    ds.upload(verbose=True)
    ds.finalize()

    return ds, metadata
