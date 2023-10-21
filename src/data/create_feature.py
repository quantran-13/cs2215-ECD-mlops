import pandas as pd
from sklearn.pipeline import Pipeline
from src.feature_pipeline.time_series_feat import creator, lag_generator


def create_feature(data: pd.DataFrame, lag_time: tuple[int, ...], warn_on_na: bool, drop_na: bool):
    preprocessing_pipeline = Pipeline(
        [
            ("time_feature_creator", creator.TimeSeriesFeatureCreator()),
            (
                "lag_feature_generator",
                lag_generator.LagFeatureGenerator(
                    lags=lag_time,
                    warn_on_na=warn_on_na,
                    drop_na=drop_na,
                ),
            ),
        ]
    )
    data.set_index("datetime_utc", inplace=True)
    data = preprocessing_pipeline.fit_transform(data)

    return data
