from sktime.transformations.base import BaseTransformer


class AttachAreaConsumerType(BaseTransformer):
    """Transformer used to extract the area and consumer type from the index to the input data."""

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        data = X.copy()
        data["area_exog"] = data.index.get_level_values(0)
        data["consumer_type_exog"] = data.index.get_level_values(1)

        return data

    def inverse_transform(self, X, y=None):
        data = X.copy()
        data = data.drop(columns=["area_exog", "consumer_type_exog"])

        return data
