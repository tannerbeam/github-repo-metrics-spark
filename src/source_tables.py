import datetime
from etl.github.expectations import ExpectationSchemas
from etl.github.org import Org
from etl.github.repo import Repo
from etl.github.contributors import Contributors
from etl.github.pulls import Pulls
from etl.spark.etl_config import get_config
from etl.spark import spark_utils

config = get_config()
spark = spark_utils.get_session()

etl_orgs = config.include_github_orgs
etl_repos = config.include_github_repos


def upsert_all():
    """
    Upsert new data into all source tables.
    Only trigger upsert for expectations once a week.
    """
    for org_name in etl_orgs:
        org = Org(org_name)
        org.upsert()

        for repo_name in etl_repos:
            if repo_name in org.list_repo_names():
                repo = Repo(org_name, repo_name)
                repo.upsert()
                contributors = Contributors(org_name, repo_name)
                contributors.upsert()
                pulls = Pulls(org_name, repo_name)
                pulls.upsert()
    
    if datetime.datetime.today().weekday() == 0:
        schemas = ExpectationSchemas()
        schemas.upsert()


def delete_all():
    """
    Delete all data from the source tables.
    """
    org = Org(etl_orgs[0])
    repo = Repo(org, etl_repos[0])
    pulls = Pulls(repo)

    org_table = org.spark_table
    repo_table = repo.spark_table
    contributors_table = contributors.spark_table
    pulls_table = pulls.spark_table

    for table in [org_table, repo_table, pulls_table]:
        if table.exists == False:
            print(f"Table `{table.name}` does not exist. No action taken.")
        else:
            table.clean_start()
            print(f"Table `{table.name}` data and metadata deleted.")