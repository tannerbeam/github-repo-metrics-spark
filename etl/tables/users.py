from etl.spark.sql_table import SqlTable

# Define the target table (write to)
name = "github_api.users"

# Define the databricks sql query needed to create table from source table(s)
select_query = """
    select
        user.id as user_id,
        user.name as user_name
    from
        (
        select
            explode(members) as user
        from
            github_api.source_orgs as
        where
            dt = (select max(dt) from github_api.source_orgs)
        )
    """

# Define the databricks sql upsert/merge logic here
# source has rows to upsert
# target gets the upsert rows
upsert_query = """
    MERGE INTO target
    USING source
    ON target.user_id = source.user_id
    WHEN MATCHED AND target.user_name != source.user_name THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """


def get_table() -> SqlTable:
    table = SqlTable(name)
    table.select_query = select_query
    table.upsert_quety = upsert_query
    return table