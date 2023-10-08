import csv
import sys
from pathlib import Path

from clearml import Dataset

CURRENT_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(CURRENT_DIR))

from root import DATA_DIR


def convert_txt_to_csv(input_file_path, output_file_path):
    with open(input_file_path, "r") as input_file:
        with open(output_file_path, "w", newline="") as output_file:
            reader = csv.reader(input_file, delimiter=";")
            writer = csv.writer(output_file)
            writer.writerows(reader)


if __name__ == "__main__":
    # Example usage:
    input_file_path = DATA_DIR / "raw" / "data_for_project.txt"
    output_file_path = DATA_DIR / "raw" / "raw.csv"
    convert_txt_to_csv(input_file_path, output_file_path)

    # Create a new Dataset
    dataset_name = "enegy_consumption"
    dataset_project = "cs2215-project"

    ds = Dataset.create(
        dataset_name=dataset_name,
        dataset_project=dataset_project,
        dataset_tags=["raw"],
    )

    # Add the data files to the Dataset
    ds.add_files(path=output_file_path, verbose=True)

    # Upload the Dataset to the backend
    ds.upload(verbose=True)

    # Finalize the Dataset (this will prevent further write operations)
    ds.finalize()
