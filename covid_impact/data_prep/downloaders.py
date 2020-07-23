"""
Summary:

Questions:
- do we want to set the urls elswehere vs explicitly using them?
(thinking in case they change)

- default path arg is pathlib.PosixPath, users should be able to pass in
simple string. best practice to cast default as string
i.e str(ext_write_path/ 'ihme')?

- removing 2020_07_11 subdiectory in extract/ihme (results from extracting the zip)

"""

import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from covid_impact.utils.utils import get_project_root


proj_root = get_project_root()
ext_write_path = str(proj_root / "data/external")


def dl_ihme(path: str = ext_write_path + "/ihme") -> None:
    """Downloads zip from ihmecovid19storage website and extracts all contents of zip
    to data/external/ihme

    :param path: Path to write the file, defaults to "../../data/external/ihme"
    :type path: str, optional
    """
    r = requests.get(
        "https://ihmecovid19storage.blob.core.windows.net/latest/ihme-covid19.zip",
        stream=True,
    )
    z = zipfile.ZipFile(io.BytesIO(r.content))

    # only csvs (avoid data dicts)
    members = [csv for csv in z.namelist() if csv.endswith(".csv")]

    # extract to path
    z.extractall(path=path, members=members)

    r.close()
    z.close()


def dl_goog_mob(path: str = ext_write_path + "/google/mobility.csv") -> None:
    """Downloads csv from covidtracking states historical api to data/external/cov_track

    :param path: Path to write the file, defaults to '../data/external/cov_track/'
    :type path: str, optional
    """
    parent = Path(path).parent
    if not parent.exists():
        Path.mkdir(parent)

    g_mob = pd.read_csv(
        "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
    )
    g_mob.to_csv(path, index=False)


def dl_covid_track(path: str = ext_write_path + "/cov_track/cov_t.csv") -> None:
    """Downloads csv from covidtracking states historical api to data/external/cov_track

    :param path: Path to write the file, defaults to '../data/external/cov_track/'
    :type path: str, optional
    """
    parent = Path(path).parent
    if not parent.exists():
        Path.mkdir(parent)

    states_daily = pd.read_csv(
        "https://covidtracking.com/api/states/daily.csv", parse_dates=["date"]
    )
    states_daily.to_csv(path, index=False)


"https://github.com/nytimes/covid-19-data/blob/master/us-states.csv"


def dl_nyt_track(path: str = ext_write_path + "/nyt_track/cov_t.csv") -> None:
    """Downloads csv from https://github.com/nytimes/covid-19-data states historical api to data/external/nyt_tracm

    :param path: Path to write the file, defaults to '../data/external/nyt_track/'
    :type path: str, optional
    """
    parent = Path(path).parent
    if not parent.exists():
        Path.mkdir(parent)

    states_daily = pd.read_csv(
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"
    ).drop(columns=["fips"])

    states_daily.to_csv(path, index=False)
