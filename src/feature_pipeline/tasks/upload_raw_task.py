import sys
import time
from pathlib import Path

import pandas as pd
from clearml import Dataset, Task, TaskTypes

CURRENT_DIR: Path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(object=CURRENT_DIR))

from configs.configs import DATASET_NAME, PROJECT_NAME
from root import DATA_DIR, FEATURE_REPO_DIR
from src.utils.logger import get_logger

logger = get_logger(logdir="logs", name=__name__)


if __name__ == "__main__":
    # Example usage:
    in_filepath: Path = DATA_DIR / "backup" / "ConsumptionDE35Hour.csv"
    df: pd.DataFrame = pd.read_csv(filepath_or_buffer=in_filepath, sep=";")
    out_filepath: Path = FEATURE_REPO_DIR / "ConsumptionDE35Hour.parquet"
    df.to_parquet(path=out_filepath, index=False)
    out_filepath: Path = FEATURE_REPO_DIR / "ConsumptionDE35Hour.csv"
    df.to_csv(path_or_buf=out_filepath, index=False)

    # data = pd.read_parquet(path=out_filepath)

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

    logger.info("=" * 80)
    print("Done!")
