import sys

from clearml import Task, TaskTypes
from pathlib import Path

CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.append(str(CURRENT_DIR))
import xgboost as xgb
from configs.configs import PROJECT_NAME
from src.data.utils import get_training_dataset
from src.models.utils import get_model
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

if __name__ == "__main__":
    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Evaluate model",
        task_type=TaskTypes.testing,
        tags="training-pipeline",
    )

    args = {
        "dataset_task_id": "",
        "model_task_id": "",
    }
    task.connect(args)
    print(f"Arguments: {args}")

    task.execute_remotely()

    X_train, X_test, y_train, y_test = get_training_dataset(
        dataset_task_id=args["dataset_task_id"]
    )
    print(f"X_test shape: {X_test.shape}")
    print(f"y_test shape: {y_test.shape}")

    model = get_model(model_task_id=args["model_task_id"])

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)

    # Print the evaluation metrics
    print(f"Mean Absolute Error (MAE): {mae:.2f}")
    print(f"Mean Squared Error (MSE): {mse:.2f}")
    print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")
    print(f"R-squared (R2): {r2:.2f}")

    # Visualize feature importance (if needed)
    xgb.plot_importance(model)
