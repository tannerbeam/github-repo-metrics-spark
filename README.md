![Github PR Metrics Example Chart](https://github.com/tannerbeam/github-repo-metrics-spark/blob/main/chart_example.png?raw=true)

# Data Workflow Github API
Databricks Spark workflow for getting data from [GitHub REST API](https://docs.github.com/en/rest?apiVersion=2022-11-28) and loading into Databricks lakehouse tables.

## Summary
Get source data from the following endpoints:
- Organizations
- Repositories
- Contributors
- Pull Requests

API response data is conditionally loaded to the `github_api` database in Databricks warehouse using upsert logic. Basically, if any new data is encountered it's inserted into the tables and if any modified data is encountered the affected rows are overwritten.

## Repo Organization

- **config**
  - etl: set etl job config values (org names, repo names, etc) here
- **etl**
  - **github**
    - api_request: general framework for making api requests with `requests` library
    - contributors: api response data from `stats/contributors` endpoint
    - org: api response data from `orgs` endpoint
    - pulls: api response data from `pulls` endpoint
    - repo: api response data from `repos` endpoint     
  - **spark**
    - date_utils: general datetime helpers
    - etl_config: class for creating dynamic config
    - spark_table: SparkTable class for working with source tables in our metastore
    - spark_utills: general databricks spark helpers
    - sql_table: SqlTable subclass of SparkTable for tables created from a sql query of source tables
  - **tables**
    - users: SqlTable for users dimension table
- **src**
  -  dag_notebook: databricks notebook with workflow code to be run with orhcestrator
  -  source_tables: etl logic for source tables (warehouse tables ending in `_source`_)
 
