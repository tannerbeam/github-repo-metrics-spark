from etl.spark import spark_utils
from etl.spark.spark_table import SparkTable
from pyspark.sql import DataFrame

spark = spark_utils.get_session()


class SqlTable(SparkTable):
    """
    Class for secondary (non-source) Github table in data warehouse
    """

    def __init__(self, name: str):
        super().__init__(name)

    @property
    def select_query(self):
        return self._select_query

    @select_query.setter
    def select_query(self, query: str):
        if not isinstance(query, str):
            raise ValueError("Select query must be a string")

        starts_with = "select"
        if not query.lower().strip().replace("\n", "").startswith(starts_with):
            raise ValueError(f"Query must start with '{starts_with}'.")

        row_count = spark.sql(query).count()
        if row_count == 0:
            raise ValueError("Select query returned zero rows.")

        else:
            self._select_query = query

    @property
    def upsert_query(self):
        return self._upsert_query

    @upsert_query.setter
    def upsert_query(self, query: str):
        if not isinstance(query, str):
            raise ValueError("Upsert query must be a string")

        starts_with = "merge"
        if not query.lower().strip().replace("\n", "").startswith(starts_with):
            raise ValueError(f"Query must start with '{starts_with}'.")

        else:
            self._upsert_query = query

    def create_from_select_query(self) -> None:
        if self.exists is True:
            raise NameError(
                f"Table `{self.name}` exists as a managed table. Use clean_start() method to delete it."
            )
        try:
            self._select_query
        except AttributeError:
            print("select_query attribute has not been set.")

        return spark.sql(f"""create table {self.name} as {self.select_query}""")

    def upsert(self) -> None:
        """
        Use delta table `merge` to upsert new/modified records
        Perm TARGET table gets merged with temp SOURCE table
        """
        # create new delta table from api query of single PR if table doesn't exist
        if self.exists == False:
            self.create_from_select_query()

        try:
            self._upsert_query
            self._upsert_query
        except AttributeError:
            print(
                "Must set 'select_query' (select ... from ...) and 'upsert_query' (merge into ...) attributes before a merge can be performed."
            )

        # SOURCE table with new records to insert into TARGET
        source_df = spark.sql(self.select_query)
        source = spark_utils.create_temp_delta_table(source_df)
        spark_utils.sql_view(source, "source")

        # TARGET table to be updated with source records
        target = self.as_delta_df()
        spark_utils.sql_view(target, "target")

        # execute the merge and save results dataframe
        # - insert new records if pull_id is new
        # - overwrite existing records if updated_ts col value has changed
        results_df = spark.sql(self.upsert_query)
        self.print_upsert_results(results_df)

    def print_upsert_results(self, results_df: DataFrame):
        results_dict = {
            item[0]: item[1] for item in results_df.collect()[0].asDict().items()
        }
        print(f"Upsert results for `{self.name}`:")
        for k, v in results_dict.items():
            print(f"{k}: {v}")