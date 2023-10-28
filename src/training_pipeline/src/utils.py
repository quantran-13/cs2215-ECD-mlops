import joblib
from clearml import Task
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def get_model(model_task_id: str):
    if model_task_id:
        model_task = Task.get_task(task_id=model_task_id)
        logger.info(f"Input task id={model_task_id} artifacts {list(model_task.artifacts.keys())}".format())  # noqa
        model_ckpt = model_task.artifacts["model"].get()
        loaded_model = joblib.load(model_ckpt)
        return loaded_model
    else:
        raise ValueError("Missing model link")
