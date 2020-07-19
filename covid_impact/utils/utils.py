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
