from clearml import Dataset
from configs.configs import DATASET_NAME, PROJECT_NAME

ds = Dataset.create(
    dataset_name=DATASET_NAME,
    dataset_project=PROJECT_NAME,
    dataset_version="1.0",
    # use_current_task=True,
    description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
    dataset_tags=["storage"],
)
ds.upload(verbose=True)
ds.finalize()

parent_datasets_id = "fc0bbd2c1878466fa2d6e554ef3c8015"
ds = Dataset.get(parent_datasets_id)
ds.list_files()
