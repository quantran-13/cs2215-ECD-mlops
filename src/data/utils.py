import pandas as pd
from clearml import Dataset, Task

from configs.configs import DATASET_NAME


def get_cleaned_dataset(dataset_task_id: str, dataset_id: str) -> pd.DataFrame:
    # ds = Dataset.get(dataset_id=dataset_id, alias=DATASET_NAME)
    # ds.get_mutable_local_copy(target_folder=PROCESSED_DIR, overwrite=True)
    # df = pd.read_csv(PROCESSED_DIR / "cleaned.csv")

    if dataset_task_id:
        dataset_cleaned_task = Task.get_task(task_id=dataset_task_id)
        print(
            f"Input task id={dataset_task_id} artifacts {list(dataset_cleaned_task.artifacts.keys())}".format()  # noqa
        )
        data_path = dataset_cleaned_task.artifacts[
            "cleaned_data"
        ].get_local_copy()
    elif dataset_id:
        ds = Dataset.get(dataset_id=dataset_id, alias=DATASET_NAME)
        # ds.get_mutable_local_copy(target_folder=PROCESSED_DIR, overwrite=True)
        data_path = ds.get_local_copy()
    else:
        raise ValueError("Missing dataset link")

    df = pd.read_csv(data_path)
    return df


def get_training_dataset(dataset_task_id: str):
    if dataset_task_id:
        dataset_task = Task.get_task(task_id=dataset_task_id)
        print(
            f"Input task id={dataset_task_id} artifacts {list(dataset_task.artifacts.keys())}".format()  # noqa
        )
        X_train = dataset_task.artifacts["X_train"].get()
        X_test = dataset_task.artifacts["X_test"].get()
        y_train = dataset_task.artifacts["y_train"].get()
        y_test = dataset_task.artifacts["y_test"].get()
    else:
        raise ValueError("Missing dataset link")

    return X_train, X_test, y_train, y_test
