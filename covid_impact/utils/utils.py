from pathlib import Path
import glob
import pandas as pd
import os

import pyspark
import pyspark.sql.functions as F


def extract_datepart(
    df: pyspark.sql.DataFrame, dt_col: str, to_extract: str, drop: bool = False
) -> pyspark.sql.DataFrame:
    """
    Base function for extracting dateparts. Used in less abstracted functions.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Base dataframe which contains ``dt_col`` column for extracting ``to_extract``.
    dt_col : str
        Name of date column to extract ``to_extract`` from.
    to_extract : str
        TODO
    drop : bool
        Whether or not to drop dt_col after extraction (default is False).

    Returns
    -------
    df : pyspark.sql.DataFrame
        df with ``to_extract`` column, optionally without original ``dt_col`` column.
    """
    df = df.withColumn(to_extract, getattr(F, to_extract)(F.col(dt_col)))
    if drop:
        df = df.drop(dt_col)
    return df


def extract_week_of_year(
    df: pyspark.sql.DataFrame, dt_col: str, drop: bool = False
) -> pyspark.sql.DataFrame:
    """
    Extract week of year from dt_col from provided df

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Base dataframe which contains ``dt_col`` column for extracting weekofyear.
    dt_col : str
        Name of date column to extract weekofyear from.
    drop : bool, optional
        Whether or not to drop dt_col after extraction (default is False).

    Returns
    -------
    df : pyspark.sql.DataFrame
        df with ``weekofyear`` column, optionally without original ``dt_col`` column.
    """
    return extract_datepart(df, dt_col, "weekofyear", drop)


def extract_year(
    df: pyspark.sql.DataFrame, dt_col: str, drop: bool = False
) -> pyspark.sql.DataFrame:
    """
    Extract year from dt_col from provided df

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Base dataframe which contains ``dt_col`` column for extracting year.
    dt_col : str
        Name of date column to extract year from.
    drop : bool, optional
        Whether or not to drop dt_col after extraction (default is False).

    Returns
    -------
    df : pyspark.sql.DataFrame
        df with ``year`` column, optionally without original ``dt_col`` column.
    """
    return extract_datepart(df, dt_col, "year", drop)


def cast(df: pyspark.sql.DataFrame, col_name: str, dtype: str) -> pyspark.sql.DataFrame:
    return df.withColumn(col_name, F.col(col_name).cast(dtype))


def cast_double(df: pyspark.sql.DataFrame, col_name: str) -> pyspark.sql.DataFrame:
    return cast(df, col_name, "double")


# Utility


def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent.parent


def get_latest_file(p: Path) -> str:
    list_of_files = glob.glob(str(p))
    latest_file_path = max(list_of_files, key=os.path.getctime)
    return latest_file_path


def all_type_check(path: str) -> str:
    # Glob looks for all types within path
    if ~path.endswith("*"):
        path += "*"
    return path


def csv_check(file: str) -> str:
    # If no .csv given in file param
    if ~file.endswith(".csv"):
        file += ".csv"
    return file


def column_check(
    df: pd.DataFrame,
    path: str = "/data/external/other/metadata/columns.csv",
    rewrite: bool = False,
) -> None:
    """Check the columns of df are exactly what is stored in the column 'columns' in the csv stored in path

    :param df: pd.DataFrame whose columns to check
    :type df: pd.DataFrame
    :param path: path of csv with 'columns' column, defaults to "data/external/other/metadata/columns.csv"
    :type path: str, optional
    :param rewrite: whether to rewrite columns.csv based on columns in df, defaults to False
    :type rewrite: bool, optional
    """
    path = str(get_project_root()) + path
    if rewrite:
        pd.DataFrame({"columns": list(df)}).to_csv(path)

    cols = pd.read_csv(path)["columns"]
    difference = set(list(df)).symmetric_difference(set(cols))
    assert len(difference) == 0, f"Columns changed. {difference}"


def read_ihme(file: str, path: str = "data/external/ihme/*") -> pd.DataFrame:
    """Return df of latest <file> named csv ihme data that was extracted to path
    * does not download from ihme *

    :param file: name of the ihme file you want to extract
    :type file: str, optional
    :param path: relative path to the file, defaults to 'data/external/ihme/*'
    :type path: str, optional
    """

    path = all_type_check(path)

    file = csv_check(file)

    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}/{file}"))

    return df


def read_goog(path: str = "data/external/google/*") -> pd.DataFrame:
    """Returns df of latest csv in google external data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/google/*'
    :type path: str, optional
    """

    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df


def read_cov_track(path: str = "data/external/cov_track/*") -> pd.DataFrame:
    """Returns df of latest csv in cov_track external data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/cov_track/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df


def read_nyt_track(path: str = "data/external/nyt_track/*") -> pd.DataFrame:
    """Returns df of latest csv in nyt_track external data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/socioeconomic/nyt_track/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df.rename(columns={"cases": "nyt_cases", "deaths": "nyt_deaths"})


def read_reg_ui(path: str = "data/external/socioeconomic/reg_claims/*") -> pd.DataFrame:
    """Returns df of latest csv in socioeconomic/reg_claims name data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/socioeconomic/reg_claims/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df


def read_pand_ui(
    path: str = "data/external/socioeconomic/pand_claims/*",
) -> pd.DataFrame:
    """Returns df of latest csv in socioeconomic/pand_claims name data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/socioeconomic/pand_claims/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df


def read_qrtly_unemp(
    path: str = "data/external/socioeconomic/unemployment_perc/*",
) -> pd.DataFrame:
    """Returns df of latest csv in socioeconomic/unemployment_perc name data path. Should be IBM/Oxford Unemployment Projections
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/socioeconomic/unemployment_perc/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df


def read_f_cip(path: str = "data/external/socioeconomic/cip/*",) -> pd.DataFrame:
    """Returns df of latest csv in socioeconomic/cip name data path
    * does not download from internet *

    :param path: relative path to the file, defaults to 'data/external/socioeconomic/cip/*'
    :type path: str, optional
    """
    path = all_type_check(path)
    # Find
    abs_path = get_project_root() / path
    latest_path = get_latest_file(abs_path)

    # Read
    df = pd.read_csv((f"{latest_path}"))

    return df
