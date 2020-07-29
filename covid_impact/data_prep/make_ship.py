import pyspark
from pyspark import SparkContext
from pyspark.sql import StructField
from pyspark.sql.functions import col, expr, lpad, next_day, to_date, weekofyear, year
from pyspark.sql.types import StringType, StructType


def read_ship_from_db(spark: pyspark.SparkContext) -> pyspark.sql.DataFrame:
    """
    Reads current (year 2020) Ship data from d4sa_us_disc.fact_spark56_daily_int_00_001 and applies proper filters
    Reads historical (year 2019) Ship data from d4sa_us_disc.fact_spark56_daily_hist_int_00_001 and applies proper filters
    and appends both of these tables together

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with pos_qty and pos_dollar
    """

    # Apply filters to discovery table so that the numbers align with publish tables that MDLZ stakeholders use for reporting / analysis
    raw_shipments_curr = (
        spark.table("d4sa_us_disc.fact_spark56_daily_int_00_001")
        .filter("D_BIC_ZMOD_IND = 'ST'")
        .filter("D_BIC_ZPHIER03 != ''")
        .filter("D_VERSION == '000'")
        .filter(
            "(D_BILL_DATE != '00000000') OR (D_BILL_DATE == '00000000' AND D_BIC_ZINFOPROV == 'ZSCNNC53')"
        )
        .filter("D_BIC_ZCAHIER01 != 'US1000012'")
        .filter("D_ZCAHIER01_T != ''")
        .filter("D_BIC_ZG_AVV004 != 0")
    )
    # Filter for only 2020 data from this table since 2019 data (esp earlier weeks) seem incomplete
    raw_shipments_curr = raw_shipments_curr.filter(
        "D_FISCPER IN ('2020001','2020002','2020003','2020004','2020005','2020006','2020007','2020008','2020009','2020010','2020011','2020012')"
    )

    raw_shipments_hist = (
        spark.table("d4sa_us_disc.fact_spark56_daily_hist_int_00_001")
        .filter("D_BIC_ZMOD_IND = 'ST'")
        .filter("D_BIC_ZPHIER03 != ''")
        .filter("D_VERSION == '000'")
        .filter(
            "(D_BILL_DATE != '00000000') OR (D_BILL_DATE == '00000000' AND D_BIC_ZINFOPROV == 'ZSCNNC53')"
        )
        .filter("D_BIC_ZCAHIER01 != 'US1000012'")
        .filter("D_ZCAHIER01_T != ''")
        .filter("D_BIC_ZG_AVV004 != 0")
    )
    raw_shipments_hist = raw_shipments_hist.filter(
        "D_FISCPER IN ('2019001','2019002','2019003','2019004','2019005','2019006','2019007','2019008','2019009','2019010','2019011','2019012')"
    )

    # Append the 2020 curr and 2019 hist DFs
    raw_shipments_hist = raw_shipments_hist.select(*raw_shipments_curr.columns)
    raw_shipments = raw_shipments_curr.union(raw_shipments_hist)

    raw_shipments.createOrReplaceTempView("fact")

    return raw_shipments


def read_customer_hierarchy_from_db(
    spark: pyspark.SparkContext,
) -> pyspark.sql.DataFrame:
    """
    Reads customer hierarchy data from hierarchies_customer_curr_hierarchy_int_00_001 and changes few columns

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with ustomer hierarchy data
    """

    # Read cust hiererachy data - for retailer name & ID

    cust_hier = spark.table("hierarchies_customer_curr_hierarchy_int_00_001")

    # Rename Dist channel ID column
    cust_hier = cust_hier.withColumnRenamed(
        "distribution_channel_id", "distribution_channel_id_cust"
    )

    # pad 0s to store_id col
    cust_hier = cust_hier.withColumn(
        "store_id", lpad(cust_hier["store_id"], 10, "0").alias("store_id")
    )
    cust_hier.createOrReplaceTempView("hierarchies_customer")

    return cust_hier


def read_product_hierarchy_from_db(
    spark: pyspark.SparkContext,
) -> pyspark.sql.DataFrame:
    """
    Reads product hierarchy data from hierarchies_product_curr_hierarchy_int_00_001 and changes few columns

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with product hierarchy data
    """

    # Translates SKU to PPG
    prod_hier = spark.table("hierarchies_product_curr_hierarchy_int_00_001")
    # Rename Dist channel ID column
    prod_hier = prod_hier.withColumnRenamed(
        "distribution_channel_id", "distribution_channel_id_prod"
    )
    # pad 0s to SKU_Id col
    prod_hier = prod_hier.withColumn(
        "sku_id", lpad(prod_hier["sku_id"], 18, "0").alias("sku_id")
    )

    prod_hier.createOrReplaceTempView("hierarchies_product")

    return prod_hier


def query_and_aggregate_data(spark: pyspark.SparkContext) -> pyspark.sql.DataFrame:

    """
    Queries the raw data from hive tables using a predefined query
    Aggregates the data by weekly
    Writes the aggregated data to hive

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with weekly aggregated data
    """

    query = """
    select
        g.date0,
        g.fiscper,
        g.promoted_product_group,
        g.promoted_product_group_desc,
        g.product_family_desc,
        g.product_segment_name,
        g.product_category_name,
        g.management_grouping_desc,
        g.state,
        g.ac_scbm_id,
        g.ac_scbm_desc,
        g.bic_zdistr_ch,
        g.state,
        sum(bic_zg_avv004) as gross_sales,
        sum(bic_znt_wt_lb) as sales_pounds
    from
        (
            select
                *
            from
                (
                    select
                        D_BIC_ZFISCDAY as date0,
                        D_FISCPER as fiscper,
                        D_CUSTOMER as customer,
                        D_MATERIAL as material,
                        D_BIC_ZDISTR_CH as bic_zdistr_ch,
                        D_REGION as state,
                        D_BIC_ZG_AVV004 as bic_zg_avv004,
                        D_BIC_ZNT_WT_LB as bic_znt_wt_lb
                    from
                        fact
                    where
                        D_FISCYEAR IN ('2020', '2019')
                ) a
                left join hierarchies_customer b on a.customer = b.store_id
                and a.bic_zdistr_ch = b.distribution_channel_id_cust
                left join hierarchies_product c on a.material = c.sku_id
                and a.bic_zdistr_ch = c.distribution_channel_id_prod
        ) g
    group by
        g.date0,
        g.fiscper,
        g.promoted_product_group,
        g.promoted_product_group_desc,
        g.product_family_desc,
        g.product_segment_name,
        g.product_category_name,
        g.management_grouping_desc,
        g.state,
        g.ac_scbm_id,
        g.ac_scbm_desc,
        g.bic_zdistr_ch,
        g.state
        """

    combined_raw_sku = spark.sql(query)
    combined_raw_sku = combined_raw_sku.withColumn(
        "date", next_day(to_date(col("date0"), "yyyyMMdd"), "saturday")
    )

    # Aggregate daily data to weekly
    agg_raw = (
        combined_raw_sku.groupBy(
            "date",
            "promoted_product_group",
            "promoted_product_group_desc",
            "product_family_desc",
            "product_segment_name",
            "product_category_name",
            "management_grouping_desc",
            "state",
            "ac_scbm_id",
            "ac_scbm_desc",
            "bic_zdistr_ch",
        )
        .agg({"gross_sales": "sum", "sales_pounds": "sum"})
        .withColumnRenamed("sum(gross_sales)", "gross_sales")
        .withColumnRenamed("sum(sales_pounds)", "sales_pounds")
    )

    agg_raw = agg_raw.cache()

    # WIP - Write out Aggregated raw table for query / QC
    agg_raw.createOrReplaceTempView("raw_ship_agg")
    spark.sql("drop table if exists default.raw_ship_agg")
    spark.sql(
        "create table if not exists default.raw_ship_agg as select * from raw_ship_agg"
    )

    # agg_raw_week = agg_raw_nodup_ppg_hier.withColumn("weekOfYear", weekofyear(col("date")))
    agg_raw_week = agg_raw.withColumn("weekOfYear", weekofyear(col("date")))
    agg_raw_week = agg_raw_week.withColumn("year", year(col("date")))

    agg_raw_week_curr = agg_raw_week.withColumn("year", expr("year - 1"))

    agg_raw_week_prev = (
        agg_raw_week.select(
            "year",
            "weekOfYear",
            "promoted_product_group",
            "promoted_product_group_desc",
            "product_family_desc",
            "product_segment_name",
            "product_category_name",
            "management_grouping_desc",
            "state",
            "ac_scbm_id",
            "ac_scbm_desc",
            "bic_zdistr_ch",
            "sales_pounds",
            "gross_sales",
        )
        .withColumnRenamed("sales_pounds", "stly_sales_pounds")
        .withColumnRenamed("gross_sales", "stly_gross_sales")
    )

    # Fill nulls with NAs to enable correct join in Spark
    agg_raw_week_curr = agg_raw_week_curr.fillna(
        "Null_Value",
        subset=[
            "promoted_product_group",
            "promoted_product_group_desc",
            "product_family_desc",
            "product_segment_name",
            "product_category_name",
            "management_grouping_desc",
            "state",
            "ac_scbm_id",
            "ac_scbm_desc",
            "bic_zdistr_ch",
        ],
    )

    agg_raw_week_prev = agg_raw_week_prev.fillna(
        "Null_Value",
        subset=[
            "promoted_product_group",
            "promoted_product_group_desc",
            "product_family_desc",
            "product_segment_name",
            "product_category_name",
            "management_grouping_desc",
            "state",
            "ac_scbm_id",
            "ac_scbm_desc",
            "bic_zdistr_ch",
        ],
    )

    # NOTE: Dataset has blank weekending dates but correct weekOfYear number.
    # Use future dates (beyond curr week) to create a weekOfYear to weekending mapping file and retrieve the weekending.
    agg_week = agg_raw_week_curr.join(
        agg_raw_week_prev,
        on=[
            "year",
            "weekOfYear",
            "promoted_product_group",
            "promoted_product_group_desc",
            "product_family_desc",
            "product_segment_name",
            "product_category_name",
            "management_grouping_desc",
            "state",
            "ac_scbm_id",
            "ac_scbm_desc",
            "bic_zdistr_ch",
        ],
        how="outer",
    )
    return agg_week


def get_channel_mapping(spark: pyspark.SparkContext) -> pyspark.sql.DataFrame:

    """
    Creates the channel mapping dataframe from the hard-coded values

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with channel mapping data
    """

    channel_mapping = spark.createDataFrame(
        [
            ("01", "Distribution Channel 01"),
            ("10", "Other"),
            ("11", "DSD Bis Intercompany"),
            ("12", "DSD Pizza Intercomp"),
            ("20", """Warehouse/Exports"""),
            ("30", "Foodservice"),
            ("40", "DSD Pizza"),
            ("45", "DSD"),
            ("50", "KFI"),
            ("55", "Plant Ingredient"),
            ("60", "Imports"),
            ("65", "Bulk FS - Specialty"),
        ],
        StructType(
            [
                StructField("bic_zdistr_ch", StringType(), True),
                StructField("channel_desc", StringType(), True),
            ]
        ),  # add your columns label here
    )

    return channel_mapping


def run_make_ship(spark: pyspark.SparkContext) -> pyspark.sql.DataFrame:

    """
    Reads the raw tables from hive
    Aggregates the data by weekly
    Does the channel mapping
    Writes the final aggregated data to hive for modeling purposes

    Parameters
    ----------
    spark : pyspark.SparkContext
        Spark context to initialize variables and get data from hive

    Returns
    -------
    pyspark.sql.DataFrame
        PySpark dataframe with aggregated data
    """

    read_ship_from_db(spark)
    read_customer_hierarchy_from_db(spark)
    read_product_hierarchy_from_db(spark)

    agg_week = query_and_aggregate_data(spark)
    channel_mapping = get_channel_mapping(spark)
    agg_raw_chn = agg_week.join(channel_mapping, how="left", on="bic_zdistr_ch")

    # Write out
    agg_out = agg_raw_chn.filter("year = 2019").filter(
        "management_grouping_desc IS NOT NULL"
    )

    agg_out.createOrReplaceTempView("agg_out")
    spark.sql("DROP TABLE IF EXISTS default.cbda_ship")
    spark.sql("create table if not exists default.cbda_ship as select * from agg_out")

    return agg_out


if __name__ == "__main__":

    """
    Main function to run all the sub-functions
    this main function would get the raw data from hive tables, process it, aggregate it and write it to hive
    """

    spark = SparkContext()
    print(spark.version)

    run_make_ship(spark)


# End of the script
