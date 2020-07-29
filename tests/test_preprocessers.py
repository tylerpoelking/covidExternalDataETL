"""Questions
    - my imports seem unessarily messy. import exceptions and validator functions?
    - reading a df in a test and modifying it? (us_state_abbrev
    """

from covid_impact.data_prep.processers import date_cols_gen
from covid_impact.data_prep.processers import usa_geo_filter
from covid_impact.utils.utils import get_project_root
from covid_impact.utils.utils import get_latest_file
from covid_impact.data_prep.validators import StateNotFoundError
from covid_impact.data_prep.validators import NullsFoundError
from covid_impact.data_prep.validators import NumUniqueError
from covid_impact.data_prep.validators import validate_usa_geo_filter
import pandas as pd
import pandas.api.types as ptypes
from pathlib import Path
import pytest

# ptypes.is_numeric_dtype to identify numeric columns,
# ptypes.is_string_dtype to identify string-like columns,
# and ptypes.is_datetime64_any_dtype to identify datetime64 columns:


def test_date_cols_gen_simple():
    right = ["date", "DATE", "this_is_a_dAtE"]
    wrong = [
        "note a daate",
        "mandate",
        "datewrong",
        "innerdateinner",
        "1date1",
        "%date%",
    ]

    df = pd.DataFrame(columns=right + wrong)

    df = date_cols_gen(df)

    for c in right:
        assert ptypes.is_datetime64_any_dtype(df[c])
    for c in wrong:
        assert not ptypes.is_datetime64_any_dtype(df[c])


def test_usa_geo_filter_missing_states():
    df = pd.DataFrame({"state": ["CA", "OH", "FL", "WA"]})
    with pytest.raises(StateNotFoundError):
        assert usa_geo_filter(df, "state")


def test_usa_geo_filter_null_in_map_file():
    us_state_abbrev = pd.read_csv(
        get_project_root() / "data/external/other/state_names.csv"
    )
    df = us_state_abbrev[["state"]]
    t = pd.DataFrame({"state": [None], "state_initial": [None]})
    us_state_abbrev = us_state_abbrev.append(t)
    with pytest.raises(StateNotFoundError):
        assert validate_usa_geo_filter(df, us_state_abbrev, "state")
