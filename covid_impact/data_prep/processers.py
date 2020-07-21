"""
    Questions:
    - use mode in usa_geo_filter to assume initialing or a better way?
    """

import pandas as pd
from covid_impact.utils.utils import get_project_root
import re


def date_cols_gen(df: pd.DataFrame) -> pd.DataFrame:
    """Returns pd.DataFrame with any column
    containing substring 'date' as datetime. Ignores capitalization

    :param df: pd.DataFrame whose cols to convert
    :type df: pd.DataFrame
    :return: df with cols whose names have string 'date' in them as dtype datetime
    :rtype: pd.DataFrame
    """
    # Get state action columns
    date_cols = [
        col
        for col in df.columns
        if bool(re.search(r"(?<![^\s_-])date(?![^\s_-])", col, flags=re.IGNORECASE))
    ]
    for d_col in date_cols:
        print(f"converting {d_col} to datetime")
        # Infer datetime in case format conventions change on ihme side
        df[d_col] = pd.to_datetime(df[d_col], infer_datetime_format=True)

    return df


def usa_geo_filter(
    df: pd.DataFrame, state_col: str, country_col: str = None, usa_val: str = None
) -> pd.DataFrame:
    """Returns df filtered to only USA in geographies of interest.
    Infers whether initial or long form col passed


    :param df: Pandas DataFrame to apply filter
    :type df: pd.DataFrame
    :param state_col: state column to filter on
    :type state_col: str
    :param country_col: country column to filter on, defaults to None
    :type country_col: str, optional
    :param usa_val: country value to filter country_col to, defaults to None
    :type usa_val: str, optional
    :return: pd.dataframe filtered to usa states and countries
    :rtype: pd.DataFrame
    """
    # Read in names columns = [state, state_initial]
    us_state_abbrev = pd.read_csv(
        get_project_root() / "data/external/other/state_names.csv"
    )

    # initial passed
    if df[state_col].str.len().mode()[0] == 2:
        state_col_new = "state_initial"
    else:
        state_col_new = "state"

    # Naming Convention
    df.rename(columns={state_col: state_col_new}, inplace=True)
    # Filter USA
    if country_col is not None:
        df = df[df[country_col] == usa_val]
    # Filter State
    df = df[df[state_col_new].isin(us_state_abbrev[state_col_new])]
    # Add other col
    df = df.merge(us_state_abbrev, how="left", on=state_col_new)

    # Assert all states in us_state_abbrev found and matched
    states_not_matched = set(us_state_abbrev[state_col_new]) - set(df[state_col_new])
    assert (
        len(set(us_state_abbrev[state_col_new]) - set(df[state_col_new])) == 0
    ), f"States not found in {state_col}: {states_not_matched}"

    # Assert no nulls in state cols
    assert not df[
        state_col_new
    ].hasnans, f"Null Values found in {state_col} after filtering to usa states"

    # move the column to head of list using index, pop and insert
    cols = list(df)
    for col in ["state", "state_initial"]:
        cols.insert(0, cols.pop(cols.index(col)))
    df = df.reindex(columns=cols)

    return df


def basic_preproc(
    df: pd.DataFrame, state_col: str, country_col: str = None, usa_val: str = None
) -> pd.DataFrame:
    """First step in Preprocessing phase:
    - removing 'V1' accidental index col
    - filtering to 51 usa states and adding state/state_intial cols
    - converting date cols
    - renaming location based on convention
    - lowercase col names and replace spaces with '_'

    :param df: Dataframe to preproccessed
    :type df: pd.DataFrame
    :param country_col: country column to filter on, defaults to None
    :type country_col: str, optional
    :param usa_val: country value to filter country_col to, defaults to None
    :type usa_val: str, optional
    :return: dataframe preproccessed
    :rtype: pd.DataFrame
    """
    # Remove uneeded col
    if "V1" in df.columns:
        df.drop("V1", axis=1, inplace=True)

    # For IHME data (remove non USA Georgia)
    if "location_id" in df.columns.values:
        df = df[df["location_id"] != 35]

    # Filter to USA + add state cols
    df = usa_geo_filter(
        df, state_col=state_col, country_col=country_col, usa_val=usa_val
    )

    # Convert date col
    df = date_cols_gen(df)

    # Lowercase all cols
    df.columns = map(lambda x: x.lower().replace(" ", "_"), df.columns)

    return df


def g_mob_preproc(g_mob: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses google mobility Pandas DataFrame.
    Expected to have already undergone basic preprocessing (basic_preproc)

    :param g_mob: initially preprocessed google mobility DataFrame
    :type g_mob: pd.DataFrame
    :return: fully preprocessed google mobility DataFrame
    :rtype: pd.DataFrame
    """

    # Filter out county and country level aggregations
    if "sub_region_2" in g_mob.columns:
        g_mob = g_mob[g_mob["sub_region_2"].isna()]
        g_mob.drop("sub_region_2", axis=1, inplace=True)
    else:
        print(
            "sub_region_2 no longer in google mobility data. Check renamed/redefined columns"
        )

    return g_mob
