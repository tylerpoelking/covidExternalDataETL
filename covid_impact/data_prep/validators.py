import pandas as pd


class state_not_found_error(Exception):
    def __init__(self, state_col: str, states_not_matched: set) -> None:
        super().__init__(f"States not found in {state_col}: {states_not_matched}")


class nulls_found_error(Exception):
    def __init__(self, col: str) -> None:
        super().__init__(f"Null Values found in {col}")


class num_unique_error(Exception):
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
    states_not_matched = set(us_state_abbrev[state_col]) - set(df[state_col])
    if not (len(set(us_state_abbrev[state_col]) - set(df[state_col])) == 0):
        raise state_not_found_error(state_col, states_not_matched)

    # Assert no nulls in state cols
    if not df[state_col].hasnans:
        raise nulls_found_error(state_col)

    # Country Tests
    if country_col is not None:
        # Assert no nulls in country col
        if not df[country_col].hasnans:
            raise nulls_found_error(country_col)

        expected_countries = 1
        actual_countries = df[country_col].nunique()
        if not actual_countries == expected_countries:
            raise num_unique_error(country_col, expected_countries, actual_countries)
