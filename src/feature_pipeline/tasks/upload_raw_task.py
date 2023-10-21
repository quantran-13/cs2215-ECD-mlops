import csv
import sys
import time
from pathlib import Path

import pandas as pd
from clearml import Dataset, Task, TaskTypes

CURRENT_DIR = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(CURRENT_DIR))

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import DATA_DIR, RAW_DIR
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def convert_txt_to_csv(input_filepath, output_filepath):
    with open(input_filepath, "r") as input_file:
        with open(output_filepath, "w", newline="") as output_file:
            reader = csv.reader(input_file, delimiter=";")
            writer = csv.writer(output_file)
            writer.writerows(reader)


if __name__ == "__main__":
    # Example usage:
    in_filepath = DATA_DIR / "backup" / "ConsumptionDE35Hour.csv"
    out_filepath = RAW_DIR / "ConsumptionDE35Hour.csv"
    convert_txt_to_csv(in_filepath, out_filepath)

    task = Task.init(
        project_name=PROJECT_NAME,
        task_name="Upload raw data",
        task_type=TaskTypes.data_processing,
        tags="data-source",
    )
    # task.execute_remotely()

    ds = Dataset.create(
        dataset_name=DATASET_NAME,
        dataset_project=PROJECT_NAME,
        dataset_tags=["raw"],
    )
    ds.add_files(path=out_filepath, verbose=True)
    ds.upload(verbose=True)
    ds.finalize()

    df = pd.read_csv(out_filepath)

    t1 = time.time()
    task.upload_artifact("feature_store", ds.id)
    task.upload_artifact("data", df)
    logger.info("Successfully uploaded data and metadata in %.2f seconds.", time.time() - t1)

    print("Done!")
