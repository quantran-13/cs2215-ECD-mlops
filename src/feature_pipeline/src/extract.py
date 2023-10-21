import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
from root import CACHED_DIR
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def from_file(
    data: pd.DataFrame,
    export_end_reference_datetime: dt.datetime | None = None,
    days_delay: int = 15,
    days_export: int = 30,
    datetime_format: str = "%Y-%m-%d %H:%M",
    cache_dir: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    """Extract data from the DK energy consumption API.

    As the official API expired in July 2023, we will use a copy of the data to simulate the same behavior.
    We have a copy of the data between '2020-06-30 22:00' and '2023-06-30 21:00'. Thus, there are 3 years of data to \
        play with.

    Here is the link to the official obsolete dataset: https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour

    Args:
        data: Source data.
        export_end_reference_datetime: The end reference datetime of the export window. If None, the current time is \
            used. Because the data is always delayed with "days_delay" days, this date is used only as a reference \
            point. The real extracted window will be computed as `[export_end_reference_datetime - days_delay - \
            days_export, export_end_reference_datetime - days_delay]`.
        days_delay: Data has a delay of N days. Thus, we have to shift our window with N days.
        days_export: The number of days to export.
        datetime_format: The datetime format of the fields from the file.
        cache_dir: The directory where the downloaded data will be cached. By default it will be downloaded in the \
            standard output directory.

    Returns:
          A tuple of a Pandas DataFrame containing the exported data and a dictionary of metadata.
    """
    export_start, export_end = _compute_extraction_window(
        export_end_reference_datetime=export_end_reference_datetime,
        days_delay=days_delay,
        days_export=days_export,
    )
    records = _extract_records_from_file(
        data=data,
        export_start=export_start,
        export_end=export_end,
        datetime_format=datetime_format,
        cache_dir=cache_dir,
    )

    metadata = {
        "days_delay": days_delay,
        "days_export": days_export,
        "export_datetime_utc_start": export_start.strftime(datetime_format),
        "export_datetime_utc_end": export_end.strftime(datetime_format),
        "datetime_format": datetime_format,
        "num_unique_samples_per_time_series": len(records["HourUTC"].unique()),
    }

    return records, metadata


def _extract_records_from_file(
    data: pd.DataFrame,
    export_start: dt.datetime,
    export_end: dt.datetime,
    datetime_format: str,
    cache_dir: Path | None = None,
) -> pd.DataFrame:
    """Extract records from the file backup based on the given export window."""
    if cache_dir is None:
        cache_dir = CACHED_DIR
    else:
        cache_dir.mkdir(parents=True, exist_ok=True)

    records = data[
        (data["HourUTC"] >= export_start.strftime(datetime_format))
        & (data["HourUTC"] < export_end.strftime(datetime_format))
    ]

    # cache the extracted data
    cache_file_path = cache_dir / f"cached_data_{export_start.strftime('%Y%m%d')}_{export_end.strftime('%Y%m%d')}.csv"
    records.to_csv(cache_file_path, index=False)

    return records


def _compute_extraction_window(
    export_end_reference_datetime: dt.datetime | None,
    days_delay: int,
    days_export: int,
) -> tuple[dt.datetime, dt.datetime]:
    """Compute the extraction window relative to 'export_end_reference_datetime' and take into consideration the \
    maximum and minimum data points available in the dataset."""
    if export_end_reference_datetime is None:
        # As the dataset will expire in July 2023, we set the export end
        # reference datetime to the last day of June 2023 + the delay.
        export_end_reference_datetime = dt.datetime(2023, 6, 30, 21, 0, 0) + dt.timedelta(days=days_delay)
        export_end_reference_datetime = export_end_reference_datetime.replace(minute=0, second=0, microsecond=0)
    else:
        export_end_reference_datetime = export_end_reference_datetime.replace(minute=0, second=0, microsecond=0)

    expiring_dataset_datetime = dt.datetime(2023, 6, 30, 21, 0, 0) + dt.timedelta(days=days_delay)
    if export_end_reference_datetime > expiring_dataset_datetime:
        export_end_reference_datetime = expiring_dataset_datetime

        logger.warning(
            "We clapped 'export_end_reference_datetime' to 'datetime(2023, 6, 30) + dt.timedelta(days=days_delay)' as \
        the dataset will not be updated starting from July 2023. The dataset will expire during 2023. \
        Check out the following link for more information: https://www.energidataservice.dk/tso-electricity/ConsumptionDE35Hour"
        )

    export_end = export_end_reference_datetime - dt.timedelta(days=days_delay)
    export_start = export_end_reference_datetime - dt.timedelta(days=days_delay + days_export)

    min_export_start = dt.datetime(2020, 6, 30, 22, 0, 0)
    if export_start < min_export_start:
        export_start = min_export_start
        export_end = export_start + dt.timedelta(days=days_export)

        logger.warning(
            "We clapped 'export_start' to 'datetime(2020, 6, 30, 22, 0, 0)' and 'export_end' to 'export_start + dt.timedelta(days=days_export)' as this is the latest window available in the dataset."
        )

    logger.info(
        "Extracting data from %s to %s", export_start.strftime("%Y-%m-%d %H:%M"), export_end.strftime("%Y-%m-%d %H:%M")
    )

    return export_start, export_end
