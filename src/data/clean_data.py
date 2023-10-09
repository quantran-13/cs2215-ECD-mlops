import pandas as pd
import sys

from clearml import Task, Dataset, TaskTypes
from pathlib import Path

CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from root import RAW_DIR, PROCESSED_DIR
from configs.configs import PROJECT_NAME, DATASET_NAME


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

    return clean_ds


task = Task.init(
    project_name=PROJECT_NAME,
    task_name="Cleaning data",
    task_type=TaskTypes.data_processing,
    tags="data-pipeline",
)

task.execute_remotely()

parameters = {"dataset_id": ""}
task.connect(parameters)

clean_ds = main(dataset_id=parameters["dataset_id"])

task.upload_artifact("cleaned_data", clean_ds.id)

print("Uploading artifacts in the background")
clean_ds.finalize()

# task.close()
print("Done!")
