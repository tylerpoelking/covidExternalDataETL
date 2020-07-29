import pandas as pd


class StateNotFoundError(Exception):
    def __init__(self, states_not_matched: set) -> None:
        super().__init__(
            f"States not matched. State in either state_names or df being preprocessed but not both: {states_not_matched}"
        )


class NullsFoundError(Exception):
    def __init__(self, col: str) -> None:
        super().__init__(f"Null Values found in {col}")


class NumUniqueError(Exception):
    def __init__(self, col: str, expect: int, actual: int) -> None:
        super().__init__(
            f"Unexpected number of unique values in {col}, expected {expect}, but is {actual}"
        )


def validate_usa_geo_filter(
    df: pd.DataFrame,
    us_state_abbrev: pd.DataFrame,
    state_col: str,
    country_col: str = None,
    usa_val: str = None,
) -> None:
    # Assert all states in us_state_abbrev found and matched
    states_not_matched = set(us_state_abbrev[state_col]).symmetric_difference(
        set(df[state_col])
    )
    assert (
        len(states_not_matched) == 0
    ), f"States not matched. State in either state_names or df being preprocessed but not both: {states_not_matched}"
    if not (len(states_not_matched) == 0):
        raise StateNotFoundError(states_not_matched)

    # Country Tests
    if country_col is not None:
        # Assert no nulls in country col
        if not df[country_col].hasnans:
            raise NullsFoundError(country_col)

        expected_countries = 1
        actual_countries = df[country_col].nunique()
        if not actual_countries == expected_countries:
            raise NumUniqueError(country_col, expected_countries, actual_countries)
