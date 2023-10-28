search_spaces = {
    "forecaster__estimator__n_jobs": [-1],
    "forecaster__estimator__n_estimators": [1000, 2000, 2500],
    "forecaster__estimator__learning_rate": [0.1, 0.15],
    "forecaster__estimator__max_depth": [-1, 5],
    "forecaster__estimator__reg_lambda": [0, 0.015],
    # "daily_season__manual_selection": Categorical([("day_of_week", "hour_of_day")]),
    # "forecaster_transformers__window_summarizer__lag_feature__lag": Categorical([tuple(range(1, 73))]),
    # "forecaster_transformers__window_summarizer__lag_feature__mean": Categorical([((1, 24), (1, 48), (1, 72))]),
    # "forecaster_transformers__window_summarizer__lag_feature__std": Categorical([((1, 24), (1, 48))]),
    # "forecaster_transformers__window_summarizer__n_jobs": Categorical([1]),
}
