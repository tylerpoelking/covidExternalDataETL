from covid_impact.utils.utils import read_ihme
from covid_impact.utils.utils import read_goog
from covid_impact.utils.utils import read_cov_track
from covid_impact.utils.utils import read_nyt_track
from covid_impact.utils.utils import read_reg_ui
from covid_impact.data_prep.downloaders import dl_ihme
from covid_impact.data_prep.downloaders import dl_goog_mob
from covid_impact.data_prep.downloaders import dl_covid_track
from covid_impact.data_prep.downloaders import dl_nyt_track
from covid_impact.data_prep.downloaders import dl_r_ui
from covid_impact.data_prep.downloaders import dl_p_ui
from covid_impact.data_prep.processers import basic_preproc
from covid_impact.data_prep.processers import g_mob_preproc
from covid_impact.data_prep.processers import c_track_preproc
from covid_impact.data_prep.processers import r_ui_preproc
from covid_impact.feat_eng.feat_engineers import fe_ihme_summary
from covid_impact.feat_eng.feat_engineers import fe_ihme_sum_to_proj
from covid_impact.feat_eng.feat_engineers import fe_goog_mob
from covid_impact.feat_eng.feat_engineers import fe_c_track
from covid_impact.utils.utils import get_project_root
import pandas as pd
from typing import Tuple


def write_interim(df: pd.DataFrame, name: str) -> None:
    """Write csv file of df to interim folder with given name (no '.csv' needed in name)

    :param df: pd.DataFrame to write
    :type df: pd.DataFrame
    :param name: filename to write (without file extension '.csv')
    :type name: str
    """
    df.to_csv(get_project_root() / f"data/interim/{name}.csv", index=False)


# def merge_data(ihme_all: pd.DataFrame, ihme_sum: pd.DataFrame, goog_mob: pd.DataFrame, c_track: pd.DataFrame, s_econ: pd.DataFrame) -> pd.DataFrame:


def merge_ihmes(
    ihme_proj_cur: pd.DataFrame,
    ihme_proj_bes: pd.DataFrame,
    ihme_proj_wor: pd.DataFrame,
    join_cols: list = ["state", "date"],
) -> pd.DataFrame:
    """Takes Preprocessed ihme projection data and combines into one DataFrame.

    :param ihme_proj_cur: Current Projections
    :type ihme_proj_cur: pd.DataFrame
    :param ihme_proj_bes: Best Projections
    :type ihme_proj_bes: pd.DataFrame
    :param ihme_proj_wor: Worst Projections
    :type ihme_proj_wor: pd.DataFrame
    :param join_cols: columns to join on, defaults to ["state", "date"]:list
    :type join_cols: [type], optional
    :return: Pandas DataFrame with all projection data, where best cols suffixed "_bes" and worst cols suffixed "_wor"
    :rtype: pd.DataFrame
    """
    # Add indicator suffix to best and worse
    overlap_cols = (
        set(ihme_proj_cur.columns)
        .intersection(set(ihme_proj_bes.columns))
        .intersection(set(ihme_proj_wor.columns))
    )

    # Remove cols we join on
    for c in join_cols:
        overlap_cols.discard(c)

    # Append Suffix for identification
    ihme_proj_bes.columns = [
        "{}{}".format(c, "" if c not in overlap_cols else "_bes")
        for c in ihme_proj_bes.columns
    ]
    ihme_proj_wor.columns = [
        "{}{}".format(c, "" if c not in overlap_cols else "_wor")
        for c in ihme_proj_wor.columns
    ]

    # Join three ihme sets
    ihme_all = ihme_proj_cur.merge(
        ihme_proj_bes, how="left", on=join_cols, validate="1:1"
    )
    ihme_all = ihme_all.merge(ihme_proj_wor, how="left", on=join_cols, validate="1:1")

    return ihme_all


def ihme_pipe() -> pd.DataFrame:
    """IHME Pipeline

    :return: ihme state summary data, ihme all projections data
    :rtype:
    """

    # Download raw unzip to file
    dl_ihme()

    # Read
    ihme_sum = read_ihme("Summary_stats_all_locs")
    ihme_proj_cur = read_ihme("Reference_hospitalization_all_locs")
    ihme_proj_wor = read_ihme("Worse_hospitalization_all_locs")
    ihme_proj_bes = read_ihme("Best_mask_hospitalization_all_locs")

    # Preproc
    ihme_sum = basic_preproc(ihme_sum, "location_name")
    ihme_proj_cur = basic_preproc(ihme_proj_cur, "location_name")
    ihme_proj_wor = basic_preproc(ihme_proj_wor, "location_name")
    ihme_proj_bes = basic_preproc(ihme_proj_bes, "location_name")

    # Merge All
    ihme_all = merge_ihmes(ihme_proj_cur, ihme_proj_bes, ihme_proj_wor)

    # Write preproc to interim
    write_interim(ihme_sum, "ihme_sum_preproc")
    write_interim(ihme_all, "ihme_all_preproc")

    # Feat Eng
    ihme_sum = fe_ihme_summary(ihme_sum)
    ihme_all = fe_ihme_sum_to_proj(ihme_proj_cur, ihme_sum)

    # Write Interim
    write_interim(ihme_all, "ihme_all_feat_eng")
    write_interim(ihme_sum, "ihme_sum_feat_eng")

    return ihme_all


def goog_mob_pipe() -> pd.DataFrame:
    """Google Mobility Pipeline

    :return: google mobility data
    :rtype: pd.DataFrame
    """

    # Download
    dl_goog_mob()

    # Read
    g_mob = read_goog()

    # Basic Preproc
    g_mob = basic_preproc(
        g_mob, "sub_region_1", country_col="country_region", usa_val="United States"
    )

    # Specific Preproc
    g_mob = g_mob_preproc(g_mob)

    # Write Interim
    write_interim(g_mob, "g_mob_preproc")

    # Feature Engineer
    g_mob = fe_goog_mob(g_mob)

    # Write Interim
    write_interim(g_mob, "g_mob_feat_eng")

    return g_mob


def covid_track_pipe() -> pd.DataFrame:
    """COVID Historical Pipeline

    :return: covid tracking data from covidtracking.com and nyt
    :rtype: pd.DataFrame
    """

    # Download
    dl_covid_track()
    dl_nyt_track()
    # Read
    c_track = read_cov_track()
    nyt_track = read_nyt_track()

    # Basic Preproc
    c_track = basic_preproc(c_track, "state")
    # Basic Preproc
    nyt_track = basic_preproc(nyt_track, "state")

    # Join Covid Tracking data with New York Times Tracking Data
    c_track = c_track.merge(
        nyt_track, on=["state", "state_initial", "date"], how="left", validate="1:1"
    )

    # Specific Preproc
    c_track = c_track_preproc(c_track)

    # Write Interim
    write_interim(c_track, "c_track_preproc")

    # Feature Engineer
    c_track = fe_c_track(c_track)

    # Write Interim
    write_interim(c_track, "c_track_feat_eng")

    return c_track


def socioecon_pipe() -> pd.DataFrame:
    """Socioeconomic Pipeline

    :return: socioeconomic data
    :rtype: pd.DataFrame
    """

    # Download
    dl_r_ui()  # Regular
    # dl_p_ui()  # Pandemic

    # Read
    r_ui = read_reg_ui()
    # p_ui = read_pand_ui()

    # Basic Preproc
    r_ui = basic_preproc(r_ui, "st")
    # p_ui = basic_preproc(p_ui, "st")

    # Specific Preproc
    r_ui = r_ui_preproc(r_ui)

    # Write Interim
    write_interim(r_ui, "r_ui_preproc")

    # Write Interim
    write_interim(r_ui, "r_ui_feat_eng")

    # Feature Engineer
    # TODO

    return r_ui


def external_refresh() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run each external dataset pipeline. Each pipeline includes:
    - Downloading latest flat data files and writing to local
    - Preprocessing data - date conversion, standardized column naming, filtering geographies, etc
    - writing interim files to local
    - feature engineering

    :return: ihme data, google mobility data, covid tracking data, socioeconomic data
    :rtype: pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame


    """
    print("Running IHME")
    ihme_all = ihme_pipe()
    print("Running google mobility")
    g_mob = goog_mob_pipe()
    print("Running covid tracking")
    c_track = covid_track_pipe()
    print("Running socioeconomic")
    s_econ = socioecon_pipe()
    print("Done with external refresh")

    return ihme_all, g_mob, c_track, s_econ


if __name__ == "__main__":
    ihme_all, goog_mob, c_track, s_econ = external_refresh()

    # Intersection check
    assert set(list(ihme_all)).intersection(set(list(goog_mob))).intersection(
        set(list(c_track))
    ).intersection(set(list(s_econ))) == {
        "date",
        "state",
        "state_initial",
    }, "Assumed column names of data not as expected, merged dataset columns may be appended with _x and_y"

    master_current_df = pd.merge(
        goog_mob,
        c_track,
        on=["state", "state_initial", "date"],
        how="left",
        validate="1:1",
    )
    master_current_df = pd.merge(
        master_current_df,
        s_econ,
        on=["state", "state_initial", "date"],
        how="left",
        validate="1:1",
    )

    master_proj = pd.merge(
        ihme_all,
        master_current_df,
        on=["state", "state_initial", "date"],
        how="left",
        validate="1:1",
    )
