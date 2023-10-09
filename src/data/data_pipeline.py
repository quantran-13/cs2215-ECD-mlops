import sys
import time
from pathlib import Path

import pandas as pd
from clearml import Dataset, Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import PROCESSED_DIR
from src.data.lag_feature_generator import LagFeatureGenerator
from src.data.time_series_processor import TimeSeriesProcessor
from src.data.utils import get_cleaned_dataset


def prepare_training_data(df, test_size: float, random_state: int):
    y = df["TotalCon"]
    X = df.drop(columns=["TotalCon"])

    X_train, X_test, y_train, y_test = train_test_split(
        X.values,
        y.values,
        test_size=test_size,
        random_state=random_state,
        shuffle=False,
    )

    return X_train, X_test, y_train, y_test


def preprocessing_data(
    df: pd.DataFrame, lag_time: int, warn_on_na: bool, drop_na: bool
) -> pd.DataFrame:
    preprocessing_pipeline = Pipeline(
        [
            ("preprocessor", TimeSeriesProcessor()),
            (
                "lag_feature_generator",
                LagFeatureGenerator(
                    lag=lag_time, warn_on_na=warn_on_na, drop_na=drop_na
                ),
            ),
        ]
    )
    transformed_data = preprocessing_pipeline.fit_transform(df)

    return transformed_data


def main(
    dataset_task_id: str,
    dataset_id: str,
    lag_time: int,
    warn_on_na: bool,
    drop_na: bool,
    test_size: float,
    random_state: int,
):
    df = get_cleaned_dataset(
        dataset_task_id=dataset_task_id, dataset_id=dataset_id
    )
    df = preprocessing_data(df, lag_time, warn_on_na, drop_na)

    save_path = PROCESSED_DIR / "processed.csv"
    df.to_csv(save_path, index=False)

    clean_ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        parent_datasets=[dataset_id],
        dataset_tags=["processed"],
    )
    clean_ds.add_files(path=save_path, verbose=True)
    clean_ds.upload(verbose=True)
    clean_ds.finalize()

    X_train, X_test, y_train, y_test = prepare_training_data(
        df, test_size, random_state
    )

    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Preprocessing data",
        task_type=TaskTypes.data_processing,
        tags="data-pipeline",
    )

    args = {
        "dataset_task_id": "f1ea4fc197364f939c593ba5e0353c0d",
        "dataset_id": "ab5cd06b064f40afae555d9033de8c29",
        "lag_time": 1,
        "warn_on_na": True,
        "drop_na": True,
        "random_state": 42,
        "test_size": 0.2,
    }
    task.connect(args)
    print(f"Arguments: {args}")

    # task.execute_remotely()

    t1 = time.time()
    print("Running preprocessing data")
    X_train, X_test, y_train, y_test = main(**args)
    print(f"Done in {time.time() - t1:.2f} seconds")

    t1 = time.time()
    print("Uploading process dataset")
    task.upload_artifact("X_train", X_train)
    task.upload_artifact("X_test", X_test)
    task.upload_artifact("y_train", y_train)
    task.upload_artifact("y_test", y_test)
    print(f"Done in {time.time() - t1:.2f} seconds")

    print("Notice, artifacts are uploaded in the background")
    print("Done")
