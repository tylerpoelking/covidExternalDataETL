import pandas as pd
from typing import Tuple


# from covid_impact.data_prep.processers import usa_geo_filter
# from covid_impact.utils.utils import get_project_root


def fe_rolling_calc(
    df: pd.DataFrame, gb: list, cols: list, window: int, type: str = "mean"
) -> pd.DataFrame:
    """Return datafram df with new rolling mean/median cols for column in cols arg,
     with a rolling mean or median (depending on type arg) over the last window arg rows.
     Assumes sorted in desired order. No sorting done by function


    :param df: pd.DataFrame with columns in cols
    :type df: pd.DataFrame
    :param gb: columns to group by. must be in df
    :type gb: list
    :param cols: cols in which to generate rolling percentages of
    :type cols: list
    :param window: Number of rows in which to calculate rolling percentages
    :type window: int
    :return: pd.DataFram with new percent change cols for col in cols. Names suffixed '{window}_perc_chg'
    :rtype: pd.DataFrame
    """
    if type == "mean":
        for c in cols:
            df[f"{c}_{window}_roll_mean"] = (
                df.groupby(gb)[c].rolling(window).mean().reset_index(0, drop=True)
            )
    if type == "median":
        for c in cols:
            df[f"{c}_{window}_roll_median"] = (
                df.groupby(gb)[c].rolling(window).median().reset_index(0, drop=True)
            )

    return df


def ihme_sum_start_end_cols(df_sum: pd.DataFrame, sbstrings: list) -> list:
    return [
        pol_col
        for pol_col in df_sum.columns
        if any(map(pol_col.__contains__, sbstrings))
    ]


def ihme_sum_policies_extract(start_end_cols: list, sbstrings: list) -> list:
    res = start_end_cols.copy()
    for i, o in enumerate(res):
        for ss in sbstrings:
            res[i] = res[i].replace(ss, "")
    return list(set(res))


def get_ihme_policies(df_sum: pd.DataFrame) -> Tuple[list, list]:
    """Extract both the original policy col names and the
    processed policy col names (without _end_date, _start_date)
    * assumes processed *

    :param df_sum: ihme all locs sunnary stats table.
    :type df_sum: pd.DataFrame
    :return: original_cols, trimmed_cols
    :rtype: list, list
    """

    # New
    pol_indicators = ["_start_date", "_end_date"]

    # Extract column names corresponding to policies and their start/end dates
    start_end_cols = ihme_sum_start_end_cols(df_sum, pol_indicators)
    policies = ihme_sum_policies_extract(start_end_cols, pol_indicators)

    # Assert Policy count
    n_expect_se = 12
    n_expect_p = n_expect_se / 2
    n_rec_se = len(start_end_cols)
    n_rec_p = len(policies)
    assert (
        n_rec_se == n_expect_se
    ), f"Expected {n_expect_se} columns with Start or End dates, received {n_rec_se}"
    assert (
        n_rec_p == n_expect_p
    ), f"Expected {n_expect_p} policy columns, received {n_rec_p}"

    return (start_end_cols, policies)


# Policies
def fe_ihme_summary(df_sum: pd.DataFrame) -> pd.DataFrame:
    """Feature Engineer for ihme summary_stats_all_locs data. Should be already preproccesed

    :param df_sum: [description]
    :type df_sum: pd.DataFrame
    :return: [description]
    :rtype: pd.DataFrame
    """

    # Policy Columns
    start_end_cols, policies = get_ihme_policies(df_sum)

    # Generate duration of policy, only populated if ended
    for policy in policies:
        df_sum[f"{policy}_duration"] = (
            df_sum[f"{policy}_end_date"] - df_sum[f"{policy}_start_date"]
        ).dt.days

    return df_sum


def fe_ihme_sum_to_proj(df_proj: pd.DataFrame, df_sum: pd.DataFrame) -> pd.DataFrame:

    # Policy Columns
    start_end_cols, policies = get_ihme_policies(df_sum)

    # Join
    df_all = df_proj.merge(
        df_sum, on=["state", "state_initial", "location_id"], how="left", validate="m:1"
    )

    # Add days_on_<policy> and on_<policy>
    for policy in policies:
        # initial days on policy
        df_all[f"days_on_{policy}"] = (
            df_all["date"] - df_all[f"{policy}_start_date"]
        ).dt.days

        # filter to 0 based on before start policy date and 0 after end date
        df_all.loc[
            (
                (df_all["date"] > df_all[f"{policy}_end_date"])
                | (df_all[f"days_on_{policy}"] < 0)
            ),
            f"days_on_{policy}",
        ] = 0

        # Add binary col for whether policy active
        df_all.loc[df_all[f"days_on_{policy}"] > 0, f"on_{policy}"] = 1
        df_all[f"on_{policy}"].fillna(0, inplace=True)

    return df_all


# Google Mobility Single Call FE function


def fe_goog_mob(g_mob: pd.DataFrame) -> pd.DataFrame:
    """Feature Engineer for google mobility data. Should be already preproccesed

    :param g_mob: Preprocessed google mobility data
    :type g_mob: pd.DataFrame
    :return: Preprocessed + feature engineered google mobility data
    :rtype: pd.DataFrame
    """

    # Get baseline cols for rolling calc
    g_mob_baselines = [col for col in g_mob.columns if "baseline" in col]
    n_expect_bl = 6
    n_rec_bl = len(g_mob_baselines)
    assert (
        n_rec_bl == n_expect_bl
    ), f"Expected {n_expect_bl} baseline columns, received {n_rec_bl}"

    # Rolling 6 window mean for baseline cals
    g_mob = fe_rolling_calc(
        g_mob, gb=["state"], cols=g_mob_baselines, window=6, type="mean"
    )

    return g_mob
