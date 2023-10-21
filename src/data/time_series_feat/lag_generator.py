import pandas as pd
from pandas.tseries.frequencies import infer_freq
from sklearn.base import BaseEstimator, TransformerMixin


class LagFeatureGenerator(BaseEstimator, TransformerMixin):
    def __init__(
        self,
        lags: tuple[int, ...] = (1, 2, 3),
        warn_on_na: bool = False,
        drop_na: bool = False,
        freq_type: str = "H",
    ):
        self.lags = lags
        self.warn_on_na = warn_on_na
        self.drop_na = drop_na
        self.freq_type = freq_type
        self.group_cols = ["area", "consumer_type"]

    def fit(self, X: pd.DataFrame, y=None):
        return self

    def __check_freq(self, X: pd.DataFrame) -> bool:
        freq = infer_freq(X.index.drop_duplicates())
        if freq == self.freq_type:
            return True
        return False

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        lagged_data = X.copy()
        lagged_data = lagged_data.sort_index(ascending=True)

        if not self.__check_freq(lagged_data):
            raise ValueError(f"Data must be indexed at {self.freq_type} frequency.")

        laf_features = [lagged_data]
        for lag in self.lags:
            lagged_column = (
                lagged_data.groupby(self.group_cols)
                .shift(lag)
                .rename(columns={"energy_consumption": f"energy_consumption_Lag_{lag}"})
            )[f"energy_consumption_Lag_{lag}"]

            check_na = lagged_column.isna()
            if self.warn_on_na and check_na.any():
                num_na = check_na.sum()
                print(f"Warning: {num_na} NaN values detected in energy_consumption_Lag_{lag} column.")

            laf_features.append(lagged_column)

        lagged_data = pd.concat(laf_features, axis=1)

        if self.drop_na:
            print("Automatically dropping rows with NaN values.")
            lagged_data = lagged_data.dropna()

        return lagged_data
