# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Issues

# COMMAND ----------

import repo_utilities as utl

# COMMAND ----------

# get issues history from github api
issues_history = utl.get_issues_history()

# COMMAND ----------

# get as pd dataframe
df_issues = utl.get_issues_dataframe(issues_history)

# convert to spark / sql
df_spark = spark.createDataFrame(df_issues)
df_spark.createOrReplaceTempView("issues_table")

# COMMAND ----------

# always drop table (small # of rows; no need for incremental)
sql = """drop table if exists github.issues"""
spark.sql(sql)

# schema
sql = """
create table if not exists github.issues (
  issue_number int comment "GitHub issue number",
  is_pull_request boolean comment "True if issue is a pull request",
  title string comment "Issue title",
  user_id string comment "GitHub user id",
  user_name string comment "GitHub user login name",
  user_is_bot boolean comment "True if user is a bot",
  user_is_core boolean comment "True if user is part of GE core team",
  labels array < string > comment "Array of issue labels",
  comment_count int comment "Number of comments on the issue",
  reaction_count int comment "Number of reactions on the issue",
  created_ts timestamp comment "Issue created timestamp",
  updated_ts timestamp comment "Issue updated timestamp",
  closed_ts timestamp comment "Issue closed timestamp",
  is_closed boolean comment "True if issue has been closed"
) comment 'GitHub Issues in OSS repo'
"""
spark.sql(sql)

# insert rows from spark tmp table
sql = """insert into github.issues select * from issues_table"""
spark.sql(sql)
