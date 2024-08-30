from etl.spark import spark_utils
from etl.spark.etl_config import get_config

from delta.tables import DeltaTable
from pyspark.sql.types import Row, StructType
from pyspark.sql.types import _parse_datatype_string as pds
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from typing import Literal
import datetime
from dataclasses import dataclass
from collections import namedtuple


spark = spark_utils.get_session()
rc = get_config()


@dataclass
class SparkTable:
    """
    Dataclass for a Databricks Spark catalog table (.delta format).
    Note these are Unity-backed MANAGED tables (not EXTERNAL).
    """

    name: str
    schema_definition: str | None = None

    def __post_init__(self):
        self.database: str = self.get_table_namespace().database
        self.table: str = self.get_table_namespace().table
        self.exists: bool = self.check_if_exists()
        self.columns: list[str] = self.get_columns()
        self.column_types: list[str] = self.get_column_types()
        self.row_count: int | None = self.get_row_count()
        self.attributes: dict[str] = {
            k: v for k, v in self.__dict__.items() if k not in ["schema_definition"]
        }

    def get_table_namespace(self) -> tuple[str]:
        """
        Make sure table name is valid.
        Returns: tuple with (database, table)
        """
        split_text = self.name.split(".")
        if not len(split_text) == 2:
            raise ValueError("name should be like 'database.table'.")

        db_name = split_text[0]
        db_list = [
            db.databaseName for db in list(spark.sql("show databases").collect())
        ]
        if db_name not in db_list:
            raise ValueError(
                f"database '{db_name}' does not exist and must be created."
            )

        TableNamespace = namedtuple("TableNamespace", "database table")

        return TableNamespace(split_text[0], split_text[1])

    def check_schema_definition(self) -> None:
        """
        Make sure schema definition has valid first line.
        """
        if not self.schema_definition:
            pass
        else:
            sw = "{0} table {1}"
            if not self.schema_definition.strip().startswith(sw):
                raise ValueError(f"Schema definition statement must start with '{sw}'.")

    def get_spark_schema(self) -> StructType | None:
        """
        Get schema as pyspark native types
        """
        if not self.exists:
            return None
        else:
            return spark.table(self.name).schema

    def get_create_table_string(self) -> str:
        """
        Get CREATE TABLE statement string based on current schema
        """
        return (
            spark.sql(f"show create table {self.name}")
            .collect()[0][0]
            .replace("\n", "")
        )

    def get_columns(self) -> list[str]:
        if not self.exists:
            return None
        else:
            return spark.table(self.name).columns

    def get_column_types(self) -> dict[str, str]:
        if not self.exists:
            return None
        else:
            return dict(spark.table(self.name).dtypes)

    def check_if_exists(self) -> bool:
        """
        Return True if table already exists in metastore
        """
        exists = True
        try:
            spark.sql(f"describe {self.name}")
        except Exception as e:
            exists = False

        return exists

    def get_row_count(self) -> int:
        """
        Get table row count
        """
        if not self.exists:
            return 0
        else:
            query = spark.sql(f"select count(*) from {self.name}")
            return query.collect()[0][0]

    def get_column_distinct_count(self, column_name: str) -> int:
        """
        Get count(distinct column_name) from table
        """
        if not self.exists or (column_name not in self.columns):
            return 0
        else:
            query = spark.sql(f"select count(distinct {column_name}) from {self.name}")
            return query.collect()[0][0]

    def get_column_min_or_max(
        self,
        column_name: str,
        min_or_max: Literal["min", "max"],
        return_datetime_as_string: bool = True,
    ) -> str | datetime.datetime | datetime.date | None:
        """
        Get min(column_name) or max(column_name).
        Return a string by default but optional to return timestamp.
        """
        assert min_or_max in [
            "min",
            "max",
        ], f"Value for 'min_or_max' arg must be 'min' or 'max'."

        if (
            not self.exists
            or (self.row_count == 0)
            or (column_name not in self.columns)
        ):
            return None

        else:
            sql = f"""
                select 
                    {min_or_max}({column_name}) as {min_or_max}_{column_name} 
                from 
                    {self.name}
            """

            value = spark.sql(sql).collect()[0][0]
            value_is_datetime = isinstance(value, datetime.datetime)
            value_is_date = isinstance(value, datetime.date)

            if return_datetime_as_string is True and any(
                [value_is_datetime, value_is_date]
            ):
                return (
                    value.strftime("%Y-%m-%d %H:%M:%S")
                    if value_is_datetime
                    else value.strftime("%Y-%m-%d")
                )
            else:
                return value

    def create_table_from_schema(self, replace_if_exists: bool = False) -> None:
        """
        Create or replace table using schema.
        """

        if self.exists and not replace_if_exists:
            raise ValueError("Table exists but 'replace_if_exists' arg is False.")

        elif self.exists and replace_if_exists:
            print(f"Existing table {self.name} replaced.")
            action = "replace"

        else:
            print(f"New table {self.name} created.")
            action = "create"

        spark.sql(self.schema_definition.format(action, self.name))
        self.exists = True
        self.last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def insert_into(
        self,
        spark_df: DataFrame,
        overwrite: bool = False,
    ) -> None:
        """
        Insert contents of an in-memory spark dataframe
        to a hive table using Pyspark `insertInto` method.
        Args:
            - spark_df: in-memory spark dataframe
            - overwrite: True to overwrite existing data in table, False to append
        Returns: None
        """
        spark_df.write.insertInto(self.name, overwrite=overwrite)

    def zorder(self, columns: list[str], quiet: bool = True) -> DataFrame:
        """
        Zorder a hive table by specfied columns
        Args:
            - hive_table: database.table to zorder
            - columns: list of column names on which to zorder
            - quiet: False to display zorder results
        Returns: None
        """

        sql = f"""optimize {self.name} zorder by ({", ".join(columns)})"""
        z_results = spark.sql(sql)

        metrics = z_results.select("metrics.*").schema.fieldNames()
        metric_cols = [f"metrics.{m}" for m in metrics]

        metrics_df = z_results.select(
            F.lit(self.name).alias("table"),
            *[F.col(mc) for mc in metric_cols],
        )

        return metrics_df

    def clean_start(self):
        """
        Vacuum delta history and drop table.
        Use with caution!
        #TODO: change to property and set to False after clean_start()
        """
        if self.exists is False:
            print(f"`{self.name}` doesn't exist as a managed table.")
        else:
            spark.sql(f"vacuum {self.name}")
            spark.sql(f"drop table {self.name}")

    def as_dataframe(self) -> DataFrame | None:
        return spark.read.table(self.name) if self.exists else None

    def as_delta(self, like_df: DataFrame | None = None) -> DeltaTable:
        """
        Get and/or create a managed delta table by name.
        If not exists, create an empty table with same schema as like_df
        """
        try:
            # will raise some SQLSTATE error if not exists or is wrong format
            delta_table = DeltaTable.forName(spark, self.name)
        except Exception:
            # create empty delta table like source if not exists
            like_df.createOrReplaceTempView("df")
            spark.sql(f"CREATE TABLE {self.name} LIKE df USING delta")
            delta_table = DeltaTable.forName(spark, self.name)

        return delta_table

    def as_delta_df(self):
        """
        Get an in-memory DELTA dataframe needed for merge ops
        """
        return self.as_delta().toDF()