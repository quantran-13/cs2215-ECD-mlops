import pandas as pd
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.dataset import PandasDataset
from src.utils.logger import get_logger

logger = get_logger("logs", __name__)


def build_expectation_suite(df: pd.DataFrame) -> ExpectationSuite:
    """Builder used to retrieve an instance of the validation expectation suite."""
    dataset = PandasDataset(df)
    expectation_suite_ec = ExpectationSuite(expectation_suite_name="energy_consumption_suite")

    # Columns.
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_table_columns_to_match_ordered_list",
            kwargs={"column_list": ["datetime_utc", "area", "consumer_type", "energy_consumption"]},
        )
    )
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(expectation_type="expect_table_column_count_to_equal", kwargs={"value": 4})
    )

    # Datetime UTC
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null", kwargs={"column": "datetime_utc"}
        )
    )

    # Area
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": "area", "value_set": list(range(0, 13))},
        )
    )
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type", kwargs={"column": "area", "type_": "int8"}
        )
    )

    # Consumer type
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={
                "column": "consumer_type",
                "value_set": [
                    111,
                    112,
                    119,
                    121,
                    122,
                    123,
                    130,
                    211,
                    212,
                    215,
                    220,
                    310,
                    320,
                    330,
                    340,
                    350,
                    360,
                    370,
                    381,
                    382,
                    390,
                    410,
                    421,
                    422,
                    431,
                    432,
                    433,
                    441,
                    442,
                    443,
                    444,
                    445,
                    446,
                    447,
                    450,
                    461,
                    462,
                    999,
                ],
            },
        )
    )
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type", kwargs={"column": "consumer_type", "type_": "int32"}
        )
    )

    # Energy consumption
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_min_to_be_between",
            kwargs={"column": "energy_consumption", "min_value": 0, "strict_min": False},
        )
    )
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "energy_consumption", "type_": "float64"},
        )
    )
    expectation_suite_ec.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "energy_consumption"},
        )
    )

    return dataset.get_expectation_suite(expectation_suite_ec)


def validate(df: pd.DataFrame, expectation_suite: ExpectationSuite) -> dict:
    """Validate the data."""
    dataset = PandasDataset(df)
    validation_results = dataset.validate(expectation_suite=expectation_suite, catch_exceptions=True)
    return validation_results.to_json_dict()
