from etl.spark import spark_utils, date_utils
from etl.spark.spark_table import SparkTable
from etl.github.api_request import http_get
from etl.github.repo import Repo

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import _parse_datatype_string as pds
from pyspark.sql import functions as F

spark = spark_utils.get_session()


class Contributors(Repo):
    """
    Class for stats/contributors api response
    """

    def __init__(self, org_name: str, repo_name: str):
        super().__init__(org_name=org_name, repo_name=repo_name)

        self.spark_table = SparkTable("github_api.source_contributors")
        
        def set_url(self):
            """
            Note: this is different from get_url_by_name('contributors')
            """
            return f"{self.get_url_by_name('url')}/stats/contributors"

        self.url = set_url(self)

    def get_contributors_json(self) -> dict:
        """
        Get api response json
        """
        return http_get(url=self.url)

    def get_contributors_data(self, contributors_json: Optional[dict] = None) -> dict:
        """
        Get cleaned api response data
        """
        if contributors_json is None:
            contributors_json = self.get_contributors_json()

        lst = []

        for r in contributors_json:
            lst.append(
                {
                    "api_request_time": date_utils.now_timestamp_string(),
                    "org_id": self.org_id,
                    "org_name": self.org_name,
                    "repo_id": self.repo_id,
                    "repo_name": self.repo_name,
                    "user_id": str(r.get("author").get("id")),
                    "user_name": r.get("author").get("login"),
                    "activity": [
                        {
                            "week": date_utils.unix_to_iso_string(d.get("w")),
                            "adds": d.get("a"),
                            "deletes": d.get("d"),
                            "commits": d.get("c"),
                        }
                        for d in r.get("weeks")
                        if (d.get("a") + d.get("d") + d.get("c")) > 0
                    ],
                }
            )
        return lst

    def get_contributors_dataframe(
        self, contributors_data: Optional[dict] = None
    ) -> DataFrame:
        """
        Cleaned api response data as spark dataframe
        """
        if contributors_data is None:
            contributors_data = self.get_contributors_data()

        schema_str: str = """
            api_request_time: STRING,
            org_id: STRING,
            org_name: STRING,
            repo_id: STRING, 
            repo_name: STRING,
            user_id: STRING, 
            user_name: STRING,
            activity: 
                ARRAY<
                    STRUCT<
                        week: STRING,
                        adds: INT, 
                        deletes: INT,
                        commits: INT
                    >
                >
        """

        df_tmp = spark.createDataFrame(data=contributors_data, schema=pds(schema_str))

        user_cols = [c for c in df_tmp.columns if c.startswith("user_")]
        activity_cols = ["adds", "deletes", "commits"]

        return df_tmp.select(
            F.col("api_request_time").cast("timestamp_ntz").alias("api_request_time"),
            F.col("org_id"),
            F.col("org_name"),
            F.col("repo_id"),
            F.col("repo_name"),
            F.col("user_id"),
            F.col("user_name"),
            F.array_min(F.col("activity.week")).cast("date").alias("first_week"),
            F.array_max(F.col("activity.week")).cast("date").alias("last_week"),
            F.array_size(F.col("activity")).alias("weeks_count"),
            *[
                F.expr(
                    f"""
                    cast(
                        aggregate(
                            activity.{ac},
                            0L,
                            (acc, x) -> acc + x 
                        )
                        as int
                    )
                    """
                ).alias(f"{ac}_count")
                for ac in activity_cols
            ],
        ).select(
            F.col("api_request_time"),
            F.col("org_id"),
            F.col("org_name"),
            F.col("repo_id"),
            F.col("repo_name"),
            F.col("user_id"),
            F.col("user_name"),
            F.col("first_week"),
            F.col("last_week"),
            F.named_struct(
                F.lit("weeks"),
                F.col("weeks_count"),
                F.lit("lines_added"),
                F.col("adds_count"),
                F.lit("lines_deleted"),
                F.col("deletes_count"),
                F.lit("commits"),
                F.col("commits_count"),
            ).alias("activity"),
        )

    def new_table_init(self, sample_dataframe: Optional[DataFrame] = None):
        """
        Trigger this to create delta new delta table if first run
        """
        print(
            f"Table {self.spark_table.name} does not exist as a managed table. Creating it from api query for a single record."
        )
        if sample_dataframe is None:
            sample_dataframe = self.get_contributors_dataframe()

        delta_table = self.spark_table.as_delta(like_df=sample_dataframe)
        print(
            f"Delta table `{self.spark_table.name}` with storage location `{self.spark_table.storage_path}` succesfully created."
        )

    def upsert(self, contributors_dataframe: Optional[DataFrame] = None) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.spark_table.exists == False:
            self.new_table_init()

        if not contributors_dataframe:
            contributors_dataframe = self.get_contributors_dataframe()

        # SOURCE table with new records to insert into TARGET
        source = spark_utils.create_temp_delta_table(contributors_dataframe)
        spark_utils.sql_view(source, "source")

        # TARGET table to be updated with source records
        target = self.spark_table.as_delta_df()
        spark_utils.sql_view(target, "target")

        # execute the merge and save results dataframe
        results_df = spark.sql(
            f"""
            MERGE INTO target
            USING source
            ON target.org_id = source.org_id
            AND target.repo_id = source.repo_id
            AND target.user_id = source.user_id
            WHEN MATCHED AND target.last_week < source.last_week THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )

        self.print_upsert_results(results_df)

    def print_upsert_results(self, results_df: DataFrame):
        results_dict = {
            item[0]: item[1] for item in results_df.collect()[0].asDict().items()
        }
        print(f"Upsert results for `{self.spark_table.name}`:")
        print(f"""Org: {self.org_name}, Repo: {self.repo_name}""")
        print("-----------------------------")
        for k, v in results_dict.items():
            print(f"{k}: {v}")