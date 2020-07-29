"""
    Questions:
    - use mode in usa_geo_filter to assume initialing or a better way?
    """

import pandas as pd
from datetime import datetime
from covid_impact.utils.utils import get_project_root
from covid_impact.data_prep.validators import validate_usa_geo_filter
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
        # Infer datetime in case format conventions change on ihme side
        df[d_col] = pd.to_datetime(df[d_col], infer_datetime_format=True)

    return df


def usa_geo_filter(
    df: pd.DataFrame, state_col: str, country_col: str = None, usa_val: str = None
) -> pd.DataFrame:
    """Returns df filtered to state_col in in USA in geographies of interest.

    Infers whether state_col is in initial form (OH, FL, etc) or long form (Ohio, Florida, etc).
    If long form, also adds state_initial column
    If initial form, also addts state (long form) column

    If df contains a column for country, will filter country_col column to be usa_val
    (default assumes no country column/filtering required)


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

    # move state and state_initial cols to leftmost column indices
    cols = list(df)
    for col in ["state", "state_initial"]:
        cols.insert(0, cols.pop(cols.index(col)))
    df = df.reindex(columns=cols)

    # Validate Expected Output
    validate_usa_geo_filter(df, us_state_abbrev, state_col_new, country_col, usa_val)

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

    # Cols - Replace Spaces
    df.columns = map(lambda x: x.replace(" ", "_"), df.columns)

    if ("state" in df.columns) and ("date" in df.columns):
        df.sort_values(["state", "date"], inplace=True, ascending=[True, True])

    return df


def g_mob_preproc(g_mob: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses google mobility Pandas DataFrame.
    Expected to have already undergone basic preprocessing (basic_preproc)

    :param g_mob: initially preprocessed google mobility DataFrame
    :type g_mob: pd.DataFrame
    :return: fully preprocessed google mobility DataFrame
    :rtype: pd.DataFrame
    """

    # Assert sub_region_2 exists
    assert (
        "sub_region_2" in g_mob.columns
    ), "sub_region_2 no longer in google mobility data. Check renamed/redefined columns"

    # Filter out county and country level aggregations
    g_mob = g_mob[g_mob["sub_region_2"].isna()]
    g_mob.drop(["census_fips_code", "sub_region_2"], axis=1, inplace=True)

    return g_mob


def c_track_preproc(c_track: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses COVID Tracking Pandas DataFrame.
    Expected to have already undergone basic preprocessing (basic_preproc)

    :param c_track: initially preprocessed Covid Tracking DataFrame
    :type c_track: pd.DataFrame
    :return: fully preprocessed Covid Tracking DataFrame
    :rtype: pd.DataFrame
    """

    c_track["lastUpdateEt"] = pd.to_datetime(c_track["lastUpdateEt"])
    max_d_checked = c_track["lastUpdateEt"].max()
    print(f"Max Day Checked: {max_d_checked}")
    if max_d_checked < pd.Timestamp.today().floor(freq="D"):
        print(
            f"Warning, covid tracking max day less than today: {max_d_checked.date()}"
        )

    # Drop columns with only one value
    one_val_cols = c_track.nunique()
    c_track.drop(
        columns=list(one_val_cols[one_val_cols == 1].index), inplace=True,
    )

    # Hard Drop uneeded cols
    c_track.drop(
        columns=["hash", "total", "dateChecked", "fips", "dateChecked", "checkTimeEt"],
        inplace=True,
    )

    # Drop cols with all nulls
    c_track.dropna(how="all", inplace=True)

    return c_track


def r_ui_preproc(r_ui: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses COVID Tracking Pandas DataFrame.
    Precprocesses the DOL Weekly Claims and Extended Benefits Trigger Data.
    Expected to have already undergone basic preprocessing (basic_preproc)

    :param r_ui: initially preprocessed Covid Tracking DataFrame
    :type r_ui: pd.DataFrame
    :return: fully preprocessed Covid Tracking DataFrame
    :rtype: pd.DataFrame
    """

    col_map = {
        "c1": "wk_num",
        "c2": "refl_wk_date",
        "c3": "IC",
        "c4": "FIC",
        "c5": "XIC",
        "c6": "WSIC",
        "c7": "WSEIC",
        "c8": "CW",
        "c9": "FCW",
        "c10": "XCW",
        "c11": "WSCW",
        "c12": "WSECW",
        "c13": "EBT",
        "c14": "EBUI",
        "c15": "ABT",
        "c16": "ABUI",
        "c17": "AT",
        "c18": "CE",
        "c19": "R",
        "c20": "AR",
        "c21": "P",
        "c22": "status",
        "c23": "status_chg_date",
    }

    r_ui.rename(columns=col_map, inplace=True)

    r_ui["date"] = pd.to_datetime(r_ui["refl_wk_date"], infer_datetime_format=True)
    # Drop columns we don't need
    drop_cols = [
        "refl_wk_date",
        "rptdate",
        "status_chg_date",
        "curdate",
        "priorwk_pub",
        "priorwk",
    ]
    r_ui.drop(drop_cols, axis=1, inplace=True)

    return r_ui


def f_cip_preproc(f_cip: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses Food CIP Pandas DataFrame.
    Precprocesses the BLS Monthly CPI for All Urban Consumers (CPI-U) U.S. city average, Food
    Expected to have already undergone basic preprocessing (basic_preproc)

    :param f_cip: DataFrame of CIP Data
    :type f_cip: pd.DataFrame
    :return: fully preprocessed U.S Food CIP DataFrame
    :rtype: pd.DataFrame
    """

    for col in ["year", "value"]:
        f_cip[col] = pd.to_numeric(f_cip[col])

    f_cip["month"] = f_cip["periodName"].apply(
        lambda x: datetime.strptime(x, "%B").month
    )
    f_cip.drop(columns=["footnotes", "periodName"], inplace=True)

    f_cip
