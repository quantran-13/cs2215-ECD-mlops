import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class LagFeatureGenerator(BaseEstimator, TransformerMixin):
    def __init__(
        self, lag: int = 1, warn_on_na: bool = False, drop_na: bool = False
    ):
        self.lag = lag
        self.warn_on_na = warn_on_na
        self.drop_na = drop_na

    def fit(self, X: pd.DataFrame, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        lagged_data = X.copy()
        for i in range(1, self.lag + 1):
            lagged_column = lagged_data.groupby("PriceArea")[
                "TotalCon"
            ].transform(lambda x: x.shift(i))
            if self.warn_on_na and lagged_column.isna().any():
                print(
                    f"Warning: NaN values detected in TotalCon_Lag_{i} column."
                )

            lagged_data[f"TotalCon_Lag_{i}"] = lagged_column

        if self.drop_na:
            print("Automatically dropping rows with NaN values.")
            lagged_data = lagged_data.dropna()

        return lagged_data
