import re
import json
import requests
from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from etl.spark import spark_utils
from etl.spark.spark_table import SparkTable
from etl.github.api_request import http_get

spark = spark_utils.get_session()


@dataclass
class ExpectationSchemas:
    """
    Utility class for working with core expectation schemas.
    """

    def __post_init__(self):
        self.paths: list[str] = self.get_from_repo()
        self.names: list[str] = self.get_from_repo(names_only=True)
        self.details_list: list[dict] = self.get_details_list()
        self.spark_table = SparkTable("reference.expectations")

    def get_from_repo(self, names_only: bool = False) -> list[str]:
        """
        Get a list of all core expectations schemas from GitHub repo.
        By default, return the repo relative paths.
        Optionally, return the expectation names only.
        """

        base_url = "https://api.github.com/repos/great-expectations/great_expectations"
        api_url = f"{base_url}/git/trees/develop?recursive=1"

        json = http_get(api_url)

        paths = [
            t.get("path")
            for t in json.get("tree")
            if t.get("path").startswith(
                "great_expectations/expectations/core/schemas/Expect"
            )
        ]

        assert len(paths) > 0, f"No core expectations schemas found in {api_url}."

        if not names_only:
            return paths
        else:
            pat = r"(Expect[A-Za-z]+)\.json"
            names = [re.search(pat, p).group(1) for p in paths]
            assert len(names) == len(
                paths
            ), f"Found {len(paths)} paths but {len(names)} names. Check regex pattern {pat}."
            return names

    def get_details_list(self, unmodified: bool = False) -> list[dict]:
        """
        Get schema info for all schemas in github repo (core/schemas)
        If unmodified=True, return the schema details without modification.
        Otherwise:
        - return the schema details with missing fields filled in with None.
        - also sort the schemas alphabetically by key
        """
        base_url = "https://raw.githubusercontent.com/great-expectations/great_expectations/develop"
        details_list = [http_get(f"{base_url}/{path}") for path in self.paths]

        if unmodified:
            return details_list
        else:
            dat = details_list

        fields = set([k for d in dat for k in list(d.keys())])

        # Add missing fields to each schema
        # This helps with spark schema inference
        for d in dat:
            for f in fields:
                if f not in d:
                    d[f] = None
        # Sort by key
        return [dict(sorted(d.items())) for d in dat]

    def get_schema_fields(self) -> set[str]:
        """
        Get set of top-level fields in schemas from across all schemas.
        """
        return set([k for dl in self.details_list for k in list(dl.keys())])

    def get_schema_properties(self) -> set[str]:
        """
        Get set of top-level properties in "properties" field in schemas from across all schemas.
        """
        return set([p for dl in self.details_list for p in dl.get("properties").keys()])

    def get_dataframe(
        self, data: list[dict] | None = None, include_legacy_expectations: bool = False
    ) -> DataFrame:
        """
        Apply transform to api request data to get a dataframe of schema details
        Optionally include legacy expectations from gallery file.
        """
        if data is None:
            data = self.details_list

        df_data = [
            {
                "title": d.get("title").replace(" ", "_"),
                "data_sources": d.get("properties")
                .get("metadata")
                .get("properties")
                .get("supported_data_sources")
                .get("const"),
                "data_quality_issues": d.get("properties")
                .get("metadata")
                .get("properties")
                .get("data_quality_issues")
                .get("const"),
            }
            for d in data
        ]

        df = spark.createDataFrame(data=df_data).select(
            F.lower(F.col("title")).alias("expectation_name"),
            F.expr(
                """
                array_sort(
                    transform(
                        data_sources, x -> lower(x)
                    )
            )"""
            ).alias("data_sources"),
            F.expr(
                """
                array_sort(
                    transform(
                        data_quality_issues, x -> lower(x)
                    )
                )"""
            ).alias("data_quality_issues"),
            F.lit(False).alias("deprecated"),
        )
        if not include_legacy_expectations:
            return df
        else:
            df_legacy = self.get_legacy_expectations_dataframe()
            return (
                df_legacy.join(df, how="left_anti", on="expectation_name")
                .union(df)
                .orderBy(["deprecated", "expectation_name"])
            )

    def get_legacy_expectations_dataframe(self) -> dict:
        """
        Get schema info from old gallery file in s3.
        Load from disk if inaccessible for whatever reason.
        """
        legacy_url = "https://superconductive-public.s3.us-east-2.amazonaws.com/static/gallery/expectation_library_v2--staging.json"

        try:
            legacy_json = requests.get(legacy_url).json()
        except json.JSONDecodeError as e:
            print(
                f"Failed to load json from '{legacy_url}'. Loading file in config/legacy_expectations.json instead."
            )
            with open(f"config/legacy_expectations.json", "r") as f:
                legacy_json = json.load(f)

        names = [ld for ld in legacy_json]

        legacy_data = [
            {
                "expectation_name": name,
                "engines": legacy_json.get(name).get("execution_engines"),
            }
            for name in names
        ]

        return spark.createDataFrame(legacy_data).select(
            F.col("expectation_name"),
            F.expr(
                """
                    transform(
                        map_keys(
                            map_filter(engines, (k,v) -> v=True)
                        ),
                    x -> 
                        case 
                            when startswith(x, 'Pandas') then 'pandas'
                            when startswith(x, 'Spark') then 'spark'
                            else 'sql'
                        end 
                    )
                """
            ).alias("data_sources"),
            F.lit(None).alias("data_quality_issues"),
            F.lit(True).alias("deprecated"),
        )

    def new_table_init(self, sample_dataframe: DataFrame | None = None):
        """
        Trigger this to create delta new delta table if first run
        """
        print(
            f"Table {self.spark_table.name} does not exist as a managed table. Triggering init to create new delta table."
        )
        if sample_dataframe is None:
            sample_dataframe = self.get_dataframe(include_legacy_expectations=True)

        delta_table = self.spark_table.as_delta(like_df=sample_dataframe)
        print(
            f"Delta table `{self.spark_table.name}` with storage location `{self.spark_table.storage_path}` succesfully created."
        )

    def upsert(self, expectations_dataframe: DataFrame | None = None) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.spark_table.exists == False:
            self.new_table_init()

        if not expectations_dataframe:
            expectations_dataframe = self.get_dataframe()

        # SOURCE table with new records to insert into TARGET
        source = spark_utils.create_temp_delta_table(expectations_dataframe)
        spark_utils.sql_view(source, "source")

        # TARGET table to be updated with source records
        target = self.spark_table.as_delta_df()
        spark_utils.sql_view(target, "target")

        # execute the merge and save results dataframe
        # - insert new if expectation from source doesn't exist in target
        # - update to deprecated if expectation from target doesn't exist in source
        # - update to new values if expecation exists in both but has been modified in source
        results_df = spark.sql(
            f"""
            MERGE INTO target USING source ON target.expectation_name = source.expectation_name
            WHEN MATCHED
            AND (target.data_sources != source.data_sources)
            OR (
                target.data_quality_issues != source.data_quality_issues
            ) THEN
            UPDATE
            SET
            *
            WHEN NOT MATCHED THEN
            INSERT
            *
            WHEN NOT MATCHED BY SOURCE and target.deprecated = false THEN
            UPDATE
            SET
            deprecated = true
            """
        )

        self.print_upsert_results(results_df)

    def print_upsert_results(self, results_df: DataFrame):
        results_dict = {
            item[0]: item[1] for item in results_df.collect()[0].asDict().items()
        }
        print(f"Upsert results for `{self.spark_table.name}`:")
        print("-----------------------------")
        for k, v in results_dict.items():
            print(f"{k}: {v}")