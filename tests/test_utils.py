from covid_impact.utils.utils import extract_week_of_year, extract_year
from covid_impact.utils.utils import cast_double
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType
import pandas as pd


spark = SparkSession.builder.getOrCreate()
df = pd.DataFrame({"date_col": ["2020-01-01"], "dbl_col": [12.345]})
df.date_col = pd.to_datetime(df.date_col)
s_df = spark.createDataFrame(df)


def test_extract_week_of_year():
    df_temp = extract_week_of_year(s_df, "date_col")
    assert df_temp.toPandas().weekofyear[0] == 1


def test_extract_year():
    df_temp = extract_year(s_df, "date_col")
    print(df_temp)
    assert df_temp.toPandas().year[0] == 2020


def test_cast_double():
    df_temp = s_df.withColumn("dbl_col", F.col("dbl_col").cast(StringType()))
    assert isinstance(df_temp.schema["dbl_col"].dataType, StringType)
    df_temp = cast_double(df_temp, "dbl_col")
    assert isinstance(df_temp.schema["dbl_col"].dataType, DoubleType)
