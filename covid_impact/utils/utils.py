import pyspark
import pyspark.sql.functions as F


def extract_datepart(
    df: pyspark.sql.DataFrame, dt_col: str, to_extract: str, drop: bool = True
) -> pyspark.sql.DataFrame:
    df = df.withColumn(to_extract, getattr(F, to_extract)(F.col(dt_col)))
    if drop:
        df = df.drop(dt_col)
    return df


def extract_week_of_year(
    df: pyspark.sql.DataFrame, dt_col: str, drop: bool = False
) -> pyspark.sql.DataFrame:
    return extract_datepart(df, dt_col, "weekofyear", drop)


def extract_year(
    df: pyspark.sql.DataFrame, dt_col: str, drop: bool = False
) -> pyspark.sql.DataFrame:
    return extract_datepart(df, dt_col, "year", drop)


def cast(df: pyspark.sql.DataFrame, col_name: str, dtype: str) -> pyspark.sql.DataFrame:
    return df.withColumn(col_name, F.col(col_name).cast(dtype))


def cast_double(df: pyspark.sql.DataFrame, col_name: str) -> pyspark.sql.DataFrame:
    return cast(df, col_name, "double")
