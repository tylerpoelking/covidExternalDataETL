from covid_impact.data_prep.processers import date_cols_gen
from covid_impact.data_prep.processers import usa_geo_filter
from covid_impact.utils.utils import get_project_root
from covid_impact.utils.utils import get_latest_file
from covid_impact.data_prep.validators import state_not_found_error

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
    with pytest.raises(state_not_found_error):
        assert usa_geo_filter(df, "state")
