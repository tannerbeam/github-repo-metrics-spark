from etl.spark import spark_utils, date_utils
from etl.spark.spark_table import SparkTable
from etl.github.api_request import http_get
from etl.github.org import Org
from etl.github.repo import Repo

from typing import Optional, Union
from random import sample

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import _parse_datatype_string as pds
from pyspark.sql import functions as F

spark = spark_utils.get_session()


class Pulls(Repo):
    """
    Class for "list pull requests" api response.
    """

    def __init__(self, org_name: str, repo_name: str):
        super().__init__(org_name=org_name, repo_name=repo_name)

        self.spark_table = SparkTable("github_api.source_pulls")

        def set_url(self) -> str:
            return self.get_url_by_name("pulls")

        self.url = set_url(self)

    def get_json(self) -> dict:
        """
        Get api response json. Fetch all PRs -- open OR closed -- from any time.
        NOTE: there is NO query param {'since': iso_8601_ts} param as with issues.
        """
        return http_get(url=self.url, params={"state": "all"})

    def get_upsert_list(self, pulls_json: Optional[dict] = None) -> list[int]:
        """
        For a given org and repo, get list of pull #'s based on time.updated field.
        """
        if pulls_json is None:
            pulls_json = self.get_json()

        query = f"""
            select 
                max(time.updated)
            from 
                {self.spark_table.name}
            where 
                1=1
                and org_id = '{self.org_id}'
                and repo_id = '{self.repo_id}'
        """

        last_update_ts = spark.sql(query).collect()[0][0]

        if last_update_ts is None:
            last_update_ts = "1970-01-01 00:00:00"
        else:
            last_update_ts = date_utils.timestamp_string_from_datetime(last_update_ts)

        return [
            pr.get("number")
            for pr in pulls_json
            if pr.get("updated_at") >= last_update_ts
        ]

    def get_metrics(self, pull_number: int | str) -> dict:
        """
        Get PR metrics from individual PR endpoint
        """
        url = f"{self.url}/{pull_number}"
        r = http_get(url)
        return {
            "comment_count": r.get("comments"),
            "review_comment_count": r.get("review_comments"),
            "commit_count": r.get("commits"),
            "changed_file_count": r.get("changed_files"),
        }

    def get_reviews(self, pull_number: int | str) -> list[dict]:
        """
        Get review info from individual PR review endpoint
        """
        url = f"{self.url}/{pull_number}/reviews"
        response = http_get(url)
        return [
            {
                "review_id": str(r.get("id")),
                "user_id": str(r.get("user").get("id")),
                "action": r.get("state").lower().replace("_", " "),
                "submitted": r.get("submitted_at"),
            }
            for r in response
        ]

    def get_data(
        self,
        pulls_json: Optional[dict] = None,
        new_only: bool = True,
        sample_only: bool = False,
    ) -> dict:
        """
        Get cleaned api response data
        Don't convert strings to datetime (use spark later).
        By default, only return new data for upsert rows.
        """
        if pulls_json is None:
            pulls_json = self.get_json()

        # NOTE: logic getting full vs incremental data from api is here.
        # full vs. incremental warehouse table updates handled in upsert() method.
        if sample_only:
            new_only = False
            pull_numbers = sample(range(0, len(pulls_json)), 3)
        elif new_only:
            sample_only = False
            pull_numbers = self.get_upsert_list(pulls_json)
        else:
            pull_numbers = [pr.get("number") for pr in pulls_json]

        lst = []
        for r in pulls_json:
            if r.get("number") in pull_numbers:
                try:
                    lst.append(
                        {
                            "api_request_time": date_utils.now_timestamp_string(),
                            "org_id": self.org_id,
                            "org_name": self.org_name,
                            "repo_id": self.repo_id,
                            "repo_name": self.repo_name,
                            "pull_id": str(r.get("id")),
                            "pull_number": str(r.get("number")),
                            "title": r.get("title"),
                            "user_id": str(r.get("user").get("id")),
                            "labels": [l.get("name") for l in r.get("labels")],
                            "is_draft": True if r.get("draft") else False,
                            "time": {
                                "created": r.get("created_at"),
                                "updated": r.get("updated_at"),
                                "closed": r.get("closed_at"),
                                "merged": r.get("merged_at"),
                            },
                            "metrics": self.get_metrics(r.get("number")),
                            "reviews": self.get_reviews(r.get("number")),
                        }
                    )
                except Exception as e:
                    print(f"Pull # {r.get('number')} failed and will be skipped.")
        return lst

    def get_dataframe(self, pulls_data: Optional[dict] = None) -> DataFrame:
        """
        Cleaned api response data as spark dataframe
        Timestamps are UTC so cast to Timestamp_NTZ type (Spark 3.4+)
        """
        if pulls_data is None:
            pulls_data = self.get_data()

        schema_str = """
            api_request_time STRING,
            org_id STRING, 
            org_name STRING,
            repo_id STRING, 
            repo_name STRING,
            pull_id STRING, 
            pull_number STRING, 
            title STRING,
            user_id STRING, 
            labels ARRAY<STRING>,
            is_draft BOOLEAN,
            time STRUCT<
                created STRING,
                updated STRING, 
                closed STRING, 
                merged STRING
            >,
            metrics STRUCT<
                        comment_count: INT,
                        review_comment_count: INT, 
                        commit_count: INT, 
                        changed_file_count: INT
                    >,
            reviews ARRAY<
                        STRUCT<
                            review_id: STRING, 
                            user_id: STRING,
                            action: STRING, 
                            submitted: STRING
                        >
                    >
        """

        df_tmp = spark.createDataFrame(data=pulls_data, schema=pds(schema_str))
        time_cols = ["created", "updated", "closed", "merged"]

        return df_tmp.select(
            F.col("api_request_time").cast("timestamp_ntz").alias("api_request_time"),
            F.col("org_id"),
            F.col("org_name"),
            F.col("repo_id"),
            F.col("repo_name"),
            F.col("pull_id"),
            F.col("pull_number"),
            F.col("title"),
            F.col("user_id"),
            F.when(F.array_size(F.col("labels")) == 0, F.lit(None))
            .otherwise(F.col("labels"))
            .alias("labels"),
            F.col("is_draft"),
            F.struct(
                *[F.col(f"time.{c}").cast("timestamp_ntz").alias(c) for c in time_cols]
            ).alias("time"),
            F.col("metrics"),
            F.when(F.array_size(F.col("reviews")) == 0, F.lit(None))
            .otherwise(
                F.expr(
                    """
                transform(
                  reviews,
                    x -> struct(
                      x.review_id,
                      x.user_id,
                      x.action,
                      cast(x.submitted as timestamp_ntz) as submitted
                    )
                )
                """
                )
            )
            .alias("reviews"),
        )

    def new_table_init(self, sample_dataframe: Optional[DataFrame] = None):
        """
        Trigger this to create delta new delta table if first run
        """
        print(
            f"Table {self.spark_table.name} does not exist as a managed table. Creating it from api query for a single record."
        )
        if sample_dataframe is None:
            sample_json = self.get_json()
            sample_data = self.get_data(sample_only=True)
            sample_dataframe = self.get_dataframe(sample_data)

        delta_table = self.spark_table.as_delta(like_df=sample_dataframe)
        print(
            f"Delta table `{self.spark_table.name}` with storage location `{self.spark_table.storage_path}` succesfully created."
        )

    def upsert(self, pulls_dataframe: Optional[DataFrame] = None) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.spark_table.exists == False:
            self.new_table_init()

        if not pulls_dataframe:
            pulls_dataframe = self.get_dataframe()

        # SOURCE table with new records to insert into TARGET
        source = spark_utils.create_temp_delta_table(pulls_dataframe)
        spark_utils.sql_view(source, "source")

        # TARGET table to be updated with source records
        target = self.spark_table.as_delta_df()
        spark_utils.sql_view(target, "target")

        # execute the merge and save results dataframe
        # - insert new records if pull_id is new
        # - overwrite existing records if updated_ts col value has changed
        results_df = spark.sql(
            f"""
            MERGE INTO target
            USING source
            ON target.repo_id = source.repo_id AND target.pull_id = source.pull_id
            WHEN MATCHED AND source.time.updated > target.time.updated THEN UPDATE SET *
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