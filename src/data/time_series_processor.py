import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import LabelEncoder


class TimeSeriesProcessor(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        # Fit label encoders
        self.label_encoders = {}
        for col in ["PriceArea", "ConsumerType_DE35"]:
            label_encoder = LabelEncoder()
            label_encoder.fit(X[col])
            self.label_encoders[col] = label_encoder
        return self

    def transform(self, X):
        # Copy the original DataFrame to avoid modifying it
        processed_df = X.copy()

        # Convert 'HourUTC' and 'HourDK' columns to datetime data type
        processed_df["HourUTC"] = pd.to_datetime(processed_df["HourUTC"])
        processed_df["HourDK"] = pd.to_datetime(processed_df["HourDK"])

        # convert to int64 due to XGBoost not support datetime
        processed_df["HourUTC"] = processed_df["HourUTC"].astype("int64")
        processed_df["HourDK"] = processed_df["HourDK"].astype("int64")

        # Ensure 'TotalCon' is of float data type
        # processed_df['TotalCon'] = processed_df['TotalCon'].astype(float) // 10**9.
        processed_df["TotalCon"] = (
            processed_df["TotalCon"].astype(float) // 10**3.0
        )

        # Encode categorical columns using the fitted label encoders
        for col in ["PriceArea", "ConsumerType_DE35"]:
            processed_df[col] = self.label_encoders[col].transform(
                processed_df[col]
            )

        return processed_df
