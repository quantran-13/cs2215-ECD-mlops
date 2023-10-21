import json
from pathlib import Path

import joblib
from clearml import Task
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def get_task_artifacts(task_id: str) -> dict:
    task = Task.get_task(task_id=task_id)
    logger.info(f"Input task id={task_id} artifacts {list(task.artifacts.keys())}".format())

    return task.artifacts


def save_json(data: dict, file_path: Path):
    """Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_path (Path): Path to save the JSON file.

    Returns: None
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_json(file_path: Path) -> dict:
    """Load a JSON file.

    Args:
        file_path (Path): Path to the JSON file.

    Returns: Dictionary with the data.
    """
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def save_model(model, model_path: str | Path):
    """
    Template for saving a model.

    Args:
        model: Trained model.
        model_path: Path to save the model.
    """
    joblib.dump(model, model_path, compress=True)


def load_model(model_path: str | Path):
    """
    Template for loading a model.

    Args:
        model_path: Path to the model.

    Returns: Loaded model.
    """
    return joblib.load(model_path)
