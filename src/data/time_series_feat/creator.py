import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class TimeSeriesFeatureCreator(BaseEstimator, TransformerMixin):
    def fit(self, X: pd.DataFrame, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # Copy the original DataFrame to avoid modifying it
        processed_df = X.copy()

        # Convert index to DatetimeIndex
        processed_df.index = pd.to_datetime(processed_df.index)

        # Create time series features based on time series index
        processed_df["hour"] = processed_df.index.hour
        processed_df["day"] = processed_df.index.day
        processed_df["week"] = processed_df.index.isocalendar().week
        processed_df["dayofweek"] = processed_df.index.dayofweek
        processed_df["month"] = processed_df.index.month
        processed_df["daysinmonth"] = processed_df.index.daysinmonth
        processed_df["quarter"] = processed_df.index.quarter
        processed_df["year"] = processed_df.index.year
        processed_df["dayofyear"] = processed_df.index.dayofyear

        return processed_df
