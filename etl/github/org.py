from etl.spark import spark_utils, date_utils
from etl.spark.spark_table import SparkTable
from etl.github.api_request import http_get

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import _parse_datatype_string as pds
from pyspark.sql import functions as F

spark = spark_utils.get_session()


class Org:
    """
    Class for github organization.
    """

    def __init__(self, org_name: str):
        def validate_org_name(org_name: str):
            """
            Validate org name and get org_id.
            Raise error if not found.
            """
            response = http_get(f"https://api.github.com/orgs/{org_name}")
            if response.get("message") == "Not Found":
                raise NameError(
                    f"Org '{org_name}' not found. Org name is case insensitive. Check spelling."
                )
            else:
                return str(response.get("id"))

        self.org_id: str = validate_org_name(org_name)
        self.org_name: str = org_name

        self.spark_table = SparkTable("github_api.source_orgs")

        def get_last_table_update(self) -> str | None:
            """
            Get the latest value of relevant datetime column.
            """
            return self.spark_table.get_column_min_or_max("dt", "max")

        self.last_table_update = get_last_table_update(self)

    def get_org_members(self) -> list[dict[str]]:
        """
        Get a list of member ids and names
        """
        members_url = f"https://api.github.com/orgs/{self.org_name}/members"
        members_json = http_get(members_url)
        return [{"id": str(m.get("id")), "name": m.get("login")} for m in members_json]

    def get_org_repos(self) -> list[dict[str]]:
        """
        Get a list of repo ids and names and privacies
        """
        repos_url = f"https://api.github.com/orgs/{self.org_name}/repos"
        repos_json = http_get(repos_url)
        return [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "is_private": r.get("private"),
                "is_fork": r.get("fork"),
                "is_archived": r.get("archived"),
                "time": {
                    "pushed": r.get("pushed_at"),
                    "created": r.get("created_at"),
                    "updated": r.get("updated_at"),
                },
            }
            for r in repos_json
        ]

    def list_repo_names(self) -> list[str]:
        """
        Get a list of repo names pertaining to org
        """
        return sorted([r.get("name") for r in self.get_org_repos()])

    def get_org_json(self) -> dict:
        """
        Raw api response data
        """
        request_url = f"https://api.github.com/orgs/{self.org_name}"

        return http_get(url=request_url)

    def get_org_data(
        self,
        org_json: Optional[dict] = None,
    ) -> dict:
        """
        Cleaned api response data
        """
        if org_json is None:
            org_json = self.get_org_json()

        return {
            "org_id": str(org_json.get("id")),
            "org_name": self.org_name,
            "members": self.get_org_members(),
            "repos": self.get_org_repos(),
            "metrics": {
                "public_repo_count": org_json.get("public_repos"),
                "private_repo_count": org_json.get("owned_private_repos"),
                "member_count": len(self.get_org_members()),
            },
            "dt": date_utils.today_date_string(),
        }

    def get_org_dataframe(self, org_data: Optional[dict] = None) -> DataFrame:
        """
        Cleaned api response data as spark dataframe
        """
        if org_data is None:
            org_data = self.get_org_data()

        schema_str = """
            org_id string,
            org_name string,
            members array<
                        struct<
                            id: string,
                            name: string
                        >
            >,
            repos array<
                    struct<
                        id: string,
                        name: string,
                        is_private: boolean,
                        is_fork: boolean,
                        is_archived: boolean,
                        time struct<
                            pushed: string,
                            created: string,
                            updated: string
                        >
                    >
            >, 
            metrics struct<
                        public_repo_count: int, 
                        private_repo_count: int, 
                        member_count: int
            >,
            dt string
        """

        return spark.createDataFrame(data=[org_data], schema=pds(schema_str)).select(
            F.col("org_id"),
            F.col("org_name"),
            F.col("members"),
            F.col("repos"),
            F.col("metrics"),
            F.col("dt").cast("date"),
        )

    def new_table_init(self):
        """
        Trigger this to create delta new delta table if first run
        """
        print(
            f"Table {self.spark_table.name} does not exist as a managed table. Creating it from api query for a single record."
        )
        sample_data = self.get_org_data()
        sample_dataframe = self.get_org_dataframe(sample_data)
        delta_table = self.spark_table.as_delta(like_df=sample_dataframe)
        print(
            f"Delta table `{self.spark_table.name}` with storage location `{self.spark_table.storage_path}` succesfully created."
        )

    def upsert(self, org_dataframe: Optional[DataFrame] = None) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.spark_table.exists == False:
            self.new_table_init()

        if not org_dataframe:
            org_dataframe = self.get_org_dataframe()

        # SOURCE table with new records to insert into TARGET
        source = spark_utils.create_temp_delta_table(org_dataframe)
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
        print(f"Org: {self.org_name}")
        print("-----------------------------")

        for k, v in results_dict.items():
            print(f"{k}: {v}")