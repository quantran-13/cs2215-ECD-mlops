import joblib
import sys

from clearml import Task, TaskTypes
from pathlib import Path
from xgboost import XGBRegressor


CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(CURRENT_DIR))

from configs.configs import PROJECT_NAME
from src.data.utils import get_training_dataset

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Training model",
        task_type=TaskTypes.training,
        tags="training-pipeline",
    )

    args = {
        "dataset_task_id": "",
        "n_estimators": 100,
        "random_state": 42,
        "enable_categorical": True,
    }
    task.connect(args)
    print(f"Arguments: {args}")

    task.execute_remotely()

    X_train, X_test, y_train, y_test = get_training_dataset(
        dataset_task_id=args["dataset_task_id"]
    )
    print(f"X_train shape: {X_train.shape}")
    print(f"y_train shape: {y_train.shape}")

    model = XGBRegressor(
        n_estimators=args["n_estimators"],
        random_state=args["random_state"],
        enable_categorical=args["enable_categorical"],
    )
    model.fit(X_train, y_train)

    joblib.dump(model, "model.pkl", compress=True)
    task.upload_artifact("model.pkl", artifact_name="model")
