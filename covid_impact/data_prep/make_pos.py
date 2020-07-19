import pyspark

from covid_impact.utils.utils import extract_week_of_year, extract_year, cast_double


def read_pos_from_db() -> pyspark.sql.DataFrame:
    q = """
        SELECT
        week_ending_date,
        retailer,
        state,
        mdlz_business,
        mdlz_category,
        mdlz_brand,
        mdlz_ppg,
        sum(pos_qty) as pos_qty,
        sum(pos_dollar) as pos_dollar
        FROM d4sa_us_disc.bluesky_pos_data
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        """
    return pyspark.sql(q)


def proc_pos(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    dt_col = "week_ending_date"
    df = extract_week_of_year(df, dt_col)
    df = extract_year(df, dt_col)

    for col in ["pos_qty", "pos_dollar"]:
        df = cast_double(df, col)

    return df


def prep_data_pos() -> None:
    pos_df = read_pos_from_db()
    pos_df = proc_pos(pos_df)
    # TODO
    # pos_df.write.mode('overwrite').parquet(TODO)


if __name__ == "__main__":
    pass
