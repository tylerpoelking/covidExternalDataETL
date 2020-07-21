from covid_impact.data_prep.processers import date_cols_gen
from covid_impact.utils.utils import get_project_root
from covid_impact.utils.utils import get_latest_file

import pandas as pd
import pandas.api.types as ptypes
from pathlib import Path

# ptypes.is_numeric_dtype to identify numeric columns,
# ptypes.is_string_dtype to identify string-like columns,
# and ptypes.is_datetime64_any_dtype to identify datetime64 columns:


def test_date_cols_gen_simple():
    right = ["date", "DATE", "this_is_a_dAtE"]
    wrong = ["note a daate", "indated"]

    df = pd.DataFrame(columns=right + wrong)

    df = date_cols_gen(df)

    for c in right:
        assert ptypes.is_datetime64_any_dtype(df[c])
    for c in wrong:
        assert not ptypes.is_datetime64_any_dtype(df[c])


# FINISH


# def test_usa_geo_filter_ihme():
#     path = "data/external/ihme/*"
#     abs_path = get_project_root() / path
#     latest_path = get_latest_file(abs_path)

#     for f in Path(latest_path).iterdir():
#         name = print(f.name)
#         df = pd.read_csv(f)
