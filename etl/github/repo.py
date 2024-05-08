from etl.spark import spark_utils, date_utils
from etl.spark.spark_table import SparkTable
from etl.github.api_request import http_get
from etl.github.org import Org

from typing import Optional
import re

from pyspark.sql import DataFrame
from pyspark.sql.types import _parse_datatype_string as pds
from pyspark.sql import functions as F

spark = spark_utils.get_session()


class Repo(Org):
    """
    Class for "get a repository" api response.
    """

    def __init__(self, org_name: str, repo_name: str):
        super().__init__(org_name=org_name)
        self.repo_name: str = self.validate_repo_name(repo_name)
        self.repo_id: str = self.get_repo_id()

        self.spark_table = SparkTable("github_api.source_repos")

        def get_last_table_update(self) -> str | None:
            """
            Get the latest value of relevant datetime column.
            """
            return self.spark_table.get_column_min_or_max("dt", "max")

        self.last_table_update = get_last_table_update(self)

    def validate_repo_name(self, repo_name: str):
        """
        Raise error if repo not found
        """
        if not repo_name in [r.get("name") for r in self.get_org_repos()]:
            raise NameError(
                f"Repo '{repo_name}' doesn't exist. Use `get_org_repos()` method on Org object for a list of available repos."
            )
        else:
            return repo_name

    def get_repo_id(self) -> str:
        return str(self.get_repo_json().get("id"))

    def list_url_names(self):
        """
        List all available repo url names.
        Use get_url_by_name() to get the full url.
        """
        repo_data = self.get_repo_data()
        return sorted([url for url in repo_data.get("urls")])

    def get_url_by_name(self, name: str = "url", show_params: bool = False):
        """
        Get a url by name.
        Get top-level url by default.
        Hide url params inside {} (e.g. /repos/owner/name/issues/{/number}) by default.
        """
        repo_data = self.get_repo_data()

        if not name in self.list_url_names():
            raise ValueError(f"URL name must be one of {self.list_url_names()}")

        url_with_params = repo_data.get("urls").get(name)
        url = re.split("\{", url_with_params)

        return url_with_params if show_params else url[0]

    def get_repo_json(self) -> dict:
        """
        Raw api response data
        """
        request_url = f"https://api.github.com/repos/{self.org_name}/{self.repo_name}"

        return http_get(url=request_url)

    def get_repo_data(self, repo_json: Optional[dict] = None) -> dict:
        """
        Cleaned api response data
        """
        if not repo_json:
            repo_json = self.get_repo_json()

        metrics_list = [
            "watchers_count",
            "open_issues_count",
            "stargazers_count",
            "subscribers_count",
            "network_count",
            "forks_count",
        ]

        return {
            "org_id": self.org_id,
            "org_name": self.org_name,
            "repo_id": self.repo_id,
            "repo_name": self.repo_name,
            "metrics": {k: int(v) for k, v in repo_json.items() if k in metrics_list},
            "urls": {
                k.replace("_url", ""): v
                for k, v in repo_json.items()
                if k.endswith("url") and v is not None
            },
            "dt": date_utils.today_date_string(),
        }

    def get_repo_dataframe(self, repo_data: Optional[dict] = None) -> DataFrame:
        """
        Cleaned api response data as spark dataframe
        """
        if not repo_data:
            repo_data = self.get_repo_data()

        schema_str = """
            org_id string,
            org_name string,
            repo_id string,
            repo_name string,
            urls map<string,string>,
            metrics struct<
                        watchers_count: int,
                        open_issues_count: int,
                        stargazers_count: int,
                        subscribers_count: int,
                        network_count: int,
                        forks_count: int
                    >,
            dt string
        """

        return spark.createDataFrame(data=[repo_data], schema=pds(schema_str)).drop(
            "urls"
        )

    def new_table_init(self):
        """
        Trigger this to create delta new delta table if first run
        """
        print(
            f"Table {self.spark_table.name} does not exist as a managed table. Creating it from api query for a single record."
        )
        sample_data = self.get_repo_data()
        sample_dataframe = self.get_repo_dataframe(sample_data)
        delta_table = self.spark_table.as_delta(like_df=sample_dataframe)
        print(
            f"Delta table `{self.spark_table.name}` with storage location `{self.spark_table.storage_path}` succesfully created."
        )

    def upsert(self, repo_dataframe: Optional[DataFrame] = None) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.spark_table.exists == False:
            self.new_table_init()

        if not repo_dataframe:
            repo_dataframe = self.get_repo_dataframe()

        # SOURCE table with new records to insert into TARGET
        source = spark_utils.create_temp_delta_table(repo_dataframe)
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
            ON target.org_id = source.org_id 
            AND target.repo_id = source.repo_id 
            AND target.dt = source.dt
            WHEN MATCHED THEN UPDATE SET *
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