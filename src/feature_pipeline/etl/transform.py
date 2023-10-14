import pandas as pd


def transform(data: pd.DataFrame) -> pd.DataFrame:
    """Transform the data to the correct format."""
    data = drop_columns(data)
    data = clean_data(data)
    data = rename_columns(data)
    data = cast_columns(data)
    data = encode_area_column(data)

    return data


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns that are not relevant for our analysis."""
    data = df.copy()

    data.drop(columns=["HourDK"], inplace=True)

    return data


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data."""
    data = df.copy()

    data.drop_duplicates(inplace=True)
    data.dropna(subset=["TotalCon"], inplace=True)
    data.reset_index(drop=True, inplace=True)

    return data


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns."""
    data = df.copy()

    data.rename(
        columns={
            "HourUTC": "datetime_utc",
            "PriceArea": "area",
            "ConsumerType_DE35": "consumer_type",
            "TotalCon": "energy_consumption",
        },
        inplace=True,
    )

    return data


def cast_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to the correct data type."""
    data = df.copy()

    data["datetime_utc"] = pd.to_datetime(data["datetime_utc"])
    data["area"] = data["area"].astype("string")
    data["consumer_type"] = data["consumer_type"].astype("int32")
    data["energy_consumption"] = data["energy_consumption"].astype("float64")

    return data


def encode_area_column(df: pd.DataFrame) -> pd.DataFrame:
    """Encode the area column to integers."""
    data = df.copy()

    area_mappings = {
        "DK": 0,
        "DK1": 1,
        "DK2": 2,
        "DE": 3,
        "SE1": 4,
        "SE2": 5,
        "SE3": 6,
        "SE4": 7,
        "NO1": 8,
        "NO2": 9,
        "NO3": 10,
        "NO4": 11,
        "NO5": 12,
    }

    data["area"] = data["area"].map(lambda string_area: area_mappings.get(string_area))
    data["area"] = data["area"].astype("int8")

    return data
