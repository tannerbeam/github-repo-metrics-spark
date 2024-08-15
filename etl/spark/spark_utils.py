from databricks.sdk.runtime import spark, dbutils
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from typing import Optional, Union
from datetime import datetime
from pandas import Timestamp as pandasTimestamp
from pandas import date_range


def get_session():
    """
    Passthru active SparkSession to importable modules
    """
    session = spark.getActiveSession()
    session.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    return session


def get_dbutils():
    """
    Use databricks sdk to get dbutils
    """
    return dbutils


def get_secret(scope: str, key: str) -> str:
    """
    Get a databricks secret via dbutils
    """
    return dbutils.secrets.get(scope, key)


def get_dt_range(
    dt_begin: str, dt_end: str, with_timestamps: bool = False
) -> list[Union[str, pandasTimestamp]]:
    """
    Use pandas date_range method to get an inclusive list of all string dates between two string dates
    Args:
        dt_begin: starting date string in %Y-%m-%d format
        dt_end: ending date string in %Y-%m-%d format
        add_timestamps: boolean for adding hh:mm:ss timestamps to the range
    Returns:
        List of %Y-%m-%d dates w or w/o timestamps. If with, last ts will be 1 sec before midnight.
    """
    assert dt_begin <= dt_end, "Begin date must be on or before end date."

    rng_pd = date_range(dt_begin, dt_end)

    if not with_timestamps:
        rng = [str(dt)[0:10] for dt in rng_pd]
        rng_type = "dates"
    else:
        rng_type = "timestamps"
        if len(rng_pd) == 1:
            rng = [
                rng_pd[0],
                rng_pd[0] + datetime.timedelta(hours=23, minutes=59, seconds=59),
            ]
        else:
            rng = [
                dt
                if not i == len(rng_pd) - 1
                else dt + datetime.timedelta(hours=23, minutes=59, seconds=59)
                for i, dt in enumerate(rng_pd)
            ]

    return rng


def select_cols(spark_df: DataFrame, cols_to_select: list[str]) -> DataFrame:
    """
    Select columns in order from a Spark DataFrame.
    Args:
        - spark_df: a Spark DataFrame
        - cols_to_select: list of column names in order of selection
    Returns: Spark DataFrame with selected columns in specified order
    """
    assert len(cols_to_select) > 0, "cols_to_select must have at least one column name."
    cols_in_df = [col for col in spark_df.columns]

    cols_in_both = list(set(cols_in_df).intersection(set(cols_to_select)))
    cols_to_ignore = list(set(cols_in_df) - set(cols_to_select))

    for c in cols_to_select:
        assert c in cols_in_df, f"Column '{c}' is not in the dataframe."

    for c in cols_to_ignore:
        print(f"Column '{c}' was not selected.")

    return spark_df.select(list(set(cols_to_select)))


def create_temp_delta_table(
    temp_df: DataFrame, temp_path: str | None = None
) -> DeltaTable:
    """
    Create a delta table in dbfs:/tmp/delta that can be used in SQL operations
    """
    spark = get_session()

    # write to DBFS by default
    if not temp_path:
        temp_path = "dbfs:/tmp/github_api_workflow"

    # rm any old files recursively and recreate dir
    dbutils.fs.rm(temp_path, True)
    dbutils.fs.mkdirs(temp_path)

    temp_df.write.format("delta").save(temp_path)

    temp_delta = DeltaTable.forPath(spark, temp_path)
    temp_df_delta = temp_delta.toDF()
    return temp_df_delta


def sql_view(spark_df: DataFrame, sql_view_name: str = "spark_df") -> None:
    """
    Create temp view from dataframe to be used in spark sql ops
    """
    return spark_df.createOrReplaceTempView(sql_view_name)