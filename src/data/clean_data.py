import sys
from pathlib import Path

import pandas as pd
from clearml import Dataset, Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR, RAW_DIR


def clean_data_func(
    raw_data_path: Path, output_file_path: Path
) -> pd.DataFrame:
    df = pd.read_csv(raw_data_path)

    df = df.drop_duplicates()

    df = df.dropna(subset=["TotalCon"])
    df["TotalCon"] = df["TotalCon"].astype(int)

    df["HourUTC"] = pd.to_datetime(df["HourUTC"])
    df.reset_index(drop=True, inplace=True)

    df.to_csv(output_file_path, index=False)

    return df


def main(dataset_id: str):
    ds = Dataset.get(dataset_id=dataset_id, alias=DATASET_NAME)
    ds.get_mutable_local_copy(target_folder=RAW_DIR, overwrite=True)

    processed_data_path = PROCESSED_DIR / "cleaned.csv"
    data_cleaned = clean_data_func(RAW_DIR / "raw.csv", processed_data_path)

    clean_ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        parent_datasets=[dataset_id],
        dataset_tags=["cleaned"],
    )
    clean_ds.add_files(path=processed_data_path, verbose=True)
    clean_ds.upload(verbose=True)
    clean_ds.finalize()

    return data_cleaned


if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Cleaning data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )
    args = {"dataset_id": "3600ab42b68b44a983d50a3397d2faae"}
    task.connect(args)

    # task.execute_remotely()

    data_cleaned = main(dataset_id=args["dataset_id"])
    task.upload_artifact("cleaned_data", data_cleaned)
    print("Uploading artifacts in the background")

    print("Done!")
