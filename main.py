from root import DATA_DIR
from configs.configs import PROJECT_NAME, DATASET_NAME, DATASET_ID


from clearml.automation import PipelineController


def pre_execute_callback_example(a_pipeline, a_node, current_param_override):
    # type (PipelineController, PipelineController.Node, dict) -> bool
    print(
        "Cloning Task id={} with parameters: {}".format(
            a_node.base_task_id, current_param_override
        )
    )
    # if we want to skip this node (and subtree of this node) we return False
    # return True to continue DAG execution
    return True


def post_execute_callback_example(a_pipeline, a_node):
    # type (PipelineController, PipelineController.Node) -> None
    print("Completed Task id={}".format(a_node.executed))
    # if we need the actual executed Task: Task.get_task(task_id=a_node.executed)
    return


pipe = PipelineController(
    name="ETL", project=PROJECT_NAME, version="0.0.1", add_pipeline_tags=False
)
pipe.set_default_execution_queue("default")

pipe.add_parameter(
    name="dataset_id",
    default=DATASET_ID,
)
pipe.add_step(
    name="clean_data",
    base_task_id="b36d38aaeb6c481a975899a5df28adbb",
    parameter_override={"General/dataset_id": "${pipeline.dataset_id}"},
)
# pipe.add_step(
#     name="stage_process",
#     base_task_project="Pipeline from Tasks example",
#     base_task_name="Pipeline step 2 process dataset",
#     parents=["stage_data"],
#     parameter_override={
#         "General/dataset_url": "${stage_data.artifacts.dataset.url}",
#         "General/test_size": 0.25,
#     },
# pre_execute_callback=pre_execute_callback_example,
# post_execute_callback=post_execute_callback_example,
# )
# pipe.add_step(
#     name="stage_train",
#     base_task_project="Pipeline from Tasks example",
#     base_task_name="Pipeline step 3 train model",
#     parents=["stage_process"],
#     parameter_override={"General/  ": "${stage_process.id}"},
# )


# For debugging purposes use local jobs
pipe.start_locally()

# Starting the pipeline (in the background)
# pipe.start()

print("Done")
