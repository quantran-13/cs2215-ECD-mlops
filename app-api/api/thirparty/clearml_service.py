from clearml import Model, Task, TaskTypes

PROJECT_NAME: str = "cs2215-project"
DATASET_NAME: str = "enegy_consumption"
REPO = "https://github.com/quantran-13/cs2215-ECD-mlops.git"  
BRANCH = "develop"

# from src.utils.logger import get_logger

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
                startedAt=task.data.started.strftime("%Y-%m-%d %H:%M:%S")
                if task.data.started
                else None,
                createdAt=task.data.created.strftime("%Y-%m-%d %H:%M:%S")
                if task.data.created
                else None,
                completedAt=task.data.completed.strftime("%Y-%m-%d %H:%M:%S")
                if task.data.completed
                else None,
                status=task.data.status.value,
                type=task.task_type.value,
                labels=m.labels,
                model_url=m.url,
                metrics=task.get_last_scalar_metrics(),
            )
            for m, task in zip(model_list, tasks)
        ]
        return model_list


    @staticmethod
    def get_extract_task(
        project_name: str = PROJECT_NAME,
        task_name:str = "Extracting data", 
        task_type=TaskTypes.data_processing,
        # tags="data-pipeline",
    ) -> Task:
        args = {
            # "artifacts_task_id": "OVERWRITE_ME",
            # "feature_store_id": "OVERWRITE_ME",
            "export_end_reference_datetime": "2023-04-01 00:00",
            "days_delay": 15,
            "days_export": 30,
            "artifacts_task_id": "3dfe30f7f8ca4619b535e43f64f66d05",
            "feature_store_id": "649430da2e0247db8ef3a073e30223b2",
        }

        task = Task.create(
            project_name=project_name,
            task_name=task_name,
            repo=REPO, 
            branch=BRANCH, 
            task_type=task_type, 
            script="./src/feature_pipeline/tasks/extract_task.py",
            working_directory=".",
            # argparse_args=args,
        )
        return task

    @staticmethod
    def get_transform_task(
        project_name: str = PROJECT_NAME,
        task_name="Transforming data",
        task_type=TaskTypes.data_processing,
        # tags="data-pipeline",
    ) -> Task:
        args = {
            # "artifacts_task_id": "OVERWRITE_ME",
            "artifacts_task_id": "58764c6b9cac4049b5d897488d3302a1",
        }

        task = Task.create(
            project_name=project_name,
            task_name=task_name,
            repo=REPO, 
            branch=BRANCH, 
            script="./src/feature_pipeline/tasks/transform_task.py",
            task_type=task_type, 
            # tags=tags, 
            working_directory=".",
            # argparse_args=args,
        )
        return task

    @staticmethod
    def get_load_task(
        project_name: str = PROJECT_NAME,
        task_name="Loading data",
        task_type=TaskTypes.data_processing,
        # tags="data-pipeline",
    ) -> Task:
        args = {
            # "artifacts_task_id": "OVERWRITE_ME",
            "artifacts_task_id": "58764c6b9cac4049b5d897488d3302a1",
        }
        task = Task.create(
            project_name=project_name,
            task_name=task_name,
            repo=REPO, 
            branch=BRANCH, 
            script="./src/feature_pipeline/tasks/load_task.py",
            task_type=task_type, 
            # tags=tags, 
            working_directory=".",
            # argparse_args=args,
        )
        return task

    @staticmethod
    def get_validate_task(
        project_name: str = PROJECT_NAME,
        task_name="Validating data",
        task_type=TaskTypes.data_processing,
        # tags="data-pipeline",
    ) -> Task:
        args = {
            # "artifacts_task_id": "OVERWRITE_ME",
            "artifacts_task_id": "384713a685d944e188769c73ccb5c6b2",
        }
        task = Task.create(
            project_name=project_name,
            task_name=task_name,
            repo=REPO, 
            branch=BRANCH, 
            script="./src/feature_pipeline/tasks/validate_task.py",
            task_type=task_type, 
            # tags=tags, 
            working_directory=".",
            # argparse_args=args,
        )
        return task



if __name__ == "__main__":
    from pprint import pprint
    pprint(
        ClearMLService.list_models_from_registry(
            project_name="cs2215-project", 
        )
    ) 