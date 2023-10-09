from configs.configs import PROJECT_NAME


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
    name="Train", project=PROJECT_NAME, version="0.0.1", add_pipeline_tags=False
)
pipe.set_default_execution_queue("default")

pipe.add_parameter(
    "dataset_task_id",
    "8c45d56eaa294c7ca120b4bfafc462d9",
)
pipe.add_parameter(
    "lag_time",
    1,
)
pipe.add_parameter(
    "lag_time",
    1,
)
pipe.add_parameter(
    "warn_on_na",
    True,
)
pipe.add_parameter(
    "drop_na",
    True,
)
pipe.add_parameter(
    "random_state",
    42,
)
pipe.add_parameter(
    "test_size",
    0.3,
)
pipe.add_parameter(
    "n_estimators",
    50,
)
pipe.add_parameter(
    "enable_categorical",
    True,
)
pipe.add_step(
    name="preprocess_data",
    base_task_id="405b6fa84b21451992aa82acac455094",
    parameter_override={
        "General/dataset_task_id": "${pipeline.dataset_task_id}",
        "General/lag_time": "${pipeline.lag_time}",
        "General/warn_on_na": "${pipeline.warn_on_na}",
        "General/drop_na": "${pipeline.drop_na}",
        "General/random_state": "${pipeline.random_state}",
        "General/test_size": "${pipeline.test_size}",
    },
    pre_execute_callback=pre_execute_callback_example,
    post_execute_callback=post_execute_callback_example,
)
pipe.add_step(
    name="training_model",
    base_task_id="979ef0f1905f4253af3cb21844654cbf",
    parents=["preprocess_data"],
    parameter_override={
        "General/dataset_task_id": "${preprocess_data.id}",
        "General/n_estimators": "${pipeline.n_estimators}",
        "General/random_state": "${pipeline.random_state}",
        "General/enable_categorical": "${pipeline.enable_categorical}",
    },
    pre_execute_callback=pre_execute_callback_example,
    post_execute_callback=post_execute_callback_example,
)
pipe.add_step(
    name="evaluate_model",
    base_task_id="659b28ddd2ca4a868d0e68986b5c6bb0",
    parents=["training_model"],
    parameter_override={
        "General/dataset_task_id": "${preprocess_data.id}",
        "General/model_task_id": "${training_model.id}",
    },
    pre_execute_callback=pre_execute_callback_example,
    post_execute_callback=post_execute_callback_example,
)


# For debugging purposes use local jobs
pipe.start_locally()

# Starting the pipeline (in the background)
# pipe.start()

print("Done")
