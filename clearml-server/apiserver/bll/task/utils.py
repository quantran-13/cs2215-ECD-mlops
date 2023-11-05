from datetime import datetime
from typing import Sequence, Union

import attr
import six

from apiserver.apierrors import errors
from apiserver.config_repo import config
from apiserver.database.errors import translate_errors_context
from apiserver.database.model.model import Model
from apiserver.database.model.project import Project
from apiserver.database.model.task.task import Task, TaskStatus, TaskSystemTags
from apiserver.database.utils import get_options
from apiserver.utilities.attrs import typed_attrs

valid_statuses = get_options(TaskStatus)
deleted_prefix = "__DELETED__"


@typed_attrs
class ChangeStatusRequest(object):
    task = attr.ib(type=Task)
    new_status = attr.ib(
        type=six.string_types, validator=attr.validators.in_(valid_statuses)
    )
    status_reason = attr.ib(type=six.string_types, default="")
    status_message = attr.ib(type=six.string_types, default="")
    force = attr.ib(type=bool, default=False)
    allow_same_state_transition = attr.ib(type=bool, default=True)
    current_status_override = attr.ib(default=None)
    user_id = attr.ib(type=str, default=None)

    def execute(self, **kwargs):
        current_status = self.current_status_override or self.task.status
        project_id = self.task.project

        # Verify new status is allowed from current status (will throw exception if not valid)
        self.validate_transition(current_status)

        control = dict(upsert=False, multi=False, write_concern=None, full_result=False)

        now = datetime.utcnow()
        fields = dict(
            status=self.new_status,
            status_reason=self.status_reason,
            status_message=self.status_message,
            status_changed=now,
            last_update=now,
            last_change=now,
            last_changed_by=self.user_id,
        )

        if self.new_status == TaskStatus.queued:
            fields["pull__system_tags"] = TaskSystemTags.development

        def safe_mongoengine_key(key):
            return f"__{key}" if key in control else key

        fields.update({safe_mongoengine_key(k): v for k, v in kwargs.items()})

        with translate_errors_context():
            # atomic change of task status by querying the task with the EXPECTED status before modifying it
            params = fields.copy()
            params.update(control)
            updated = Task.objects(id=self.task.id, status=current_status).update(
                **params
            )

        if not updated:
            # failed to change status (someone else beat us to it?)
            raise errors.bad_request.FailedChangingTaskStatus(
                task_id=self.task.id,
                current_status=current_status,
                new_status=self.new_status,
            )

        update_project_time(project_id)

        # make sure that _raw_ queries are not returned back to the client
        fields.pop("__raw__", None)

        return dict(updated=updated, fields=fields)

    def validate_transition(self, current_status):
        if self.force:
            return
        if self.new_status != current_status:
            validate_status_change(current_status, self.new_status)
        elif not self.allow_same_state_transition:
            raise errors.bad_request.InvalidTaskStatus(
                "Task already in requested status",
                current_status=current_status,
                new_status=self.new_status,
            )


def validate_status_change(current_status, new_status):
    assert current_status in valid_statuses
    assert new_status in valid_statuses

    allowed_statuses = get_possible_status_changes(current_status)
    if new_status not in allowed_statuses:
        raise errors.bad_request.InvalidTaskStatus(
            "Invalid status change",
            current_status=current_status,
            new_status=new_status,
        )


state_machine = {
    TaskStatus.created: {TaskStatus.queued, TaskStatus.in_progress},
    TaskStatus.queued: {TaskStatus.created, TaskStatus.in_progress, TaskStatus.stopped},
    TaskStatus.in_progress: {
        TaskStatus.stopped,
        TaskStatus.failed,
        TaskStatus.created,
        TaskStatus.completed,
    },
    TaskStatus.stopped: {
        TaskStatus.closed,
        TaskStatus.created,
        TaskStatus.failed,
        TaskStatus.queued,
        TaskStatus.in_progress,
        TaskStatus.published,
        TaskStatus.publishing,
        TaskStatus.completed,
    },
    TaskStatus.closed: {
        TaskStatus.created,
        TaskStatus.failed,
        TaskStatus.published,
        TaskStatus.publishing,
        TaskStatus.stopped,
    },
    TaskStatus.failed: {TaskStatus.created, TaskStatus.stopped, TaskStatus.published},
    TaskStatus.publishing: {TaskStatus.published},
    TaskStatus.published: set(),
    TaskStatus.completed: {
        TaskStatus.published,
        TaskStatus.in_progress,
        TaskStatus.created,
    },
}


def get_possible_status_changes(current_status):
    """
    :param current_status:
    :return possible states from current state
    """
    possible = state_machine.get(current_status)
    if possible is None:
        raise errors.server_error.InternalError(
            f"Current status {current_status} not supported by state machine"
        )

    return possible


def update_project_time(project_ids: Union[str, Sequence[str]]):
    if not project_ids:
        return

    if isinstance(project_ids, str):
        project_ids = [project_ids]

    return Project.objects(id__in=project_ids).update(last_update=datetime.utcnow())


def get_task_for_update(
    company_id: str, task_id: str, allow_all_statuses: bool = False, force: bool = False
) -> Task:
    """
    Loads only task id and return the task only if it is updatable (status == 'created')
    """
    task = Task.get_for_writing(company=company_id, id=task_id, _only=("id", "status"))
    if not task:
        raise errors.bad_request.InvalidTaskId(id=task_id)

    if allow_all_statuses:
        return task

    allowed_statuses = (
        [TaskStatus.created, TaskStatus.in_progress] if force else [TaskStatus.created]
    )
    if task.status not in allowed_statuses:
        raise errors.bad_request.InvalidTaskStatus(
            expected=TaskStatus.created, status=task.status
        )
    return task


def update_task(
    task: Task, user_id: str, update_cmds: dict, set_last_update: bool = True
):
    now = datetime.utcnow()
    last_updates = dict(last_change=now, last_changed_by=user_id)
    if set_last_update:
        last_updates.update(last_update=now)
    return task.update(**update_cmds, **last_updates)


def get_last_metric_updates(
    task_id: str,
    last_scalar_events: dict,
    raw_updates: dict,
    extra_updates: dict,
    model_events: bool = False,
):
    max_values = config.get("services.tasks.max_last_metrics", 2000)
    total_metrics = set()
    if max_values:
        query = dict(id=task_id)
        to_add = sum(len(v) for m, v in last_scalar_events.items())
        if to_add <= max_values:
            query[f"unique_metrics__{max_values - to_add}__exists"] = True
        db_cls = Model if model_events else Task
        task = db_cls.objects(**query).only("unique_metrics").first()
        if task and task.unique_metrics:
            total_metrics = set(task.unique_metrics)

    new_metrics = []

    def add_last_metric_conditional_update(
        metric_path: str, metric_value, iter_value: int, is_min: bool
    ):
        """
        Build an aggregation for an atomic update of the min or max value and the corresponding iteration
        """
        if is_min:
            field_prefix = "min"
            op = "$gt"
        else:
            field_prefix = "max"
            op = "$lt"

        value_field = f"{metric_path}__{field_prefix}_value".replace("__", ".")
        condition = {
            "$or": [
                {"$lte": [f"${value_field}", None]},
                {op: [f"${value_field}", metric_value]},
            ]
        }
        raw_updates[value_field] = {
            "$cond": [condition, metric_value, f"${value_field}"]
        }

        value_iteration_field = f"{metric_path}__{field_prefix}_value_iteration".replace(
            "__", "."
        )
        raw_updates[value_iteration_field] = {
            "$cond": [condition, iter_value, f"${value_iteration_field}"]
        }

    for metric_key, metric_data in last_scalar_events.items():
        for variant_key, variant_data in metric_data.items():
            metric = f"{variant_data.get('metric')}/{variant_data.get('variant')}"
            if max_values:
                if len(total_metrics) >= max_values and metric not in total_metrics:
                    continue
                total_metrics.add(metric)

            new_metrics.append(metric)
            path = f"last_metrics__{metric_key}__{variant_key}"
            for key, value in variant_data.items():
                if key in ("min_value", "max_value"):
                    add_last_metric_conditional_update(
                        metric_path=path,
                        metric_value=value,
                        iter_value=variant_data.get(f"{key}_iter", 0),
                        is_min=(key == "min_value"),
                    )
                elif key in ("metric", "variant", "value"):
                    extra_updates[f"set__{path}__{key}"] = value

    if new_metrics:
        extra_updates["add_to_set__unique_metrics"] = new_metrics
