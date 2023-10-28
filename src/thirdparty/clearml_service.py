# from src.utils.logger import get_logger

from clearml import Model, Task

# logger = get_logger("logs", __name__)


class ClearMLService:
    @staticmethod
    def get_task_artifacts(task_id: str) -> dict:
        task = Task.get_task(task_id=task_id)
        # logger.info(f"Input task id={task_id} artifacts {list(task.artifacts.keys())}".format())
        return task.artifacts

    @staticmethod
    def list_models_from_registry(
        project_name: str | None = None,
        model_name: str | None = None,
        max_results: int | None = 30,
        tags: list[str] | None = None,
        only_published: bool = False,
        include_archived: bool = False,
        metadata: dict[str, str] | None = None,
    ) -> list[dict]:
        model_list = Model.query_models(
            project_name=project_name,
            model_name=model_name,
            tags=tags,
            only_published=only_published,
            include_archived=include_archived,
            max_results=max_results,
            metadata=metadata,
        )
        tasks = [Task.get_task(task_id=m.task) for m in model_list]
        model_list = [
            dict(
                model_id=m.id,
                model_name=m.name,
                project_id=m.project,
                project_name=project_name,
                tags=m.tags,
                task_id=m.task,
                task_name=task.name,
                startedAt=task.data.started.strftime("%Y-%m-%d %H:%M:%S") if task.data.started else None,
                createdAt=task.data.created.strftime("%Y-%m-%d %H:%M:%S") if task.data.created else None,
                completedAt=task.data.completed.strftime("%Y-%m-%d %H:%M:%S") if task.data.completed else None,
                status=task.data.status.value,
                type=task.task_type.value,
                labels=m.labels,
                model_url=m.url,
                metrics=task.get_last_scalar_metrics(),
                # task_page_url=f"{config.clearml.host}/projects/{m.project}/experiments/{m.task}",
                # project_page_url=f"{config.clearml.host}/projects/{m.project}/experiments",
            )
            for m, task in zip(model_list, tasks)
        ]
        return model_list


if __name__ == "__main__":
    from pprint import pprint

    pprint(
        ClearMLService.list_models_from_registry(
            project_name="cs2215-project",
        )
    )
