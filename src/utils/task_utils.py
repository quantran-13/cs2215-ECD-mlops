import json
from pathlib import Path

from clearml import Task


def get_task_artifacts(task_id: str) -> dict:
    task = Task.get_task(task_id=task_id)
    print(f"Input task id={task_id} artifacts {list(task.artifacts.keys())}".format())

    return task.artifacts


def save_json(data: dict, file_path: Path):
    """
    Save a dictionary as a JSON file.

    Args:
        data: data to save.
        file_path (Path): Path to save the JSON file.

    Returns: None
    """
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_json(file_path: Path) -> dict:
    """
    Load a JSON file.

    Args:
        file_path (Path): Path to the JSON file.

    Returns: Dictionary with the data.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)
