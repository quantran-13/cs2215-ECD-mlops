from clearml import Dataset
from configs.configs import DATASET_NAME, PROJECT_NAME

ds = Dataset.create(
    dataset_name=DATASET_NAME,
    dataset_project=PROJECT_NAME,
    dataset_version="1.0",
    # use_current_task=True,
    description="Denmark hourly energy consumption data. Data is uploaded with an 15 days delay.",
    dataset_tags=["fv-storage"],
)
ds.upload(verbose=True)
ds.finalize()

parent_datasets_id = "1b11612f969f45918f1a5014c1a13157"
ds = Dataset.get(parent_datasets_id)
ds.list_files()
