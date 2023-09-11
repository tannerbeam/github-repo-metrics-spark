# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Pulls

# COMMAND ----------

import repo_utilities as utl

# COMMAND ----------

# Get pull history from git api as a list
# note: there is no query parameter to get limit date range
# as such, should take a few minutes to get the full history
pull_history = utl.get_pulls_history()

# COMMAND ----------

# Filter and select pull history and put into dataframe
df_pulls = utl.get_pulls_dataframe(pull_history)

#convert to spark / sql
df_spark = spark.createDataFrame(df_pulls)
df_spark.createOrReplaceTempView("pulls_table")

# COMMAND ----------

# always drop table (small # of rows; no need for incremental)
sql = """drop table if exists github.pulls"""
spark.sql(sql)

# schema 
sql = """
create table if not exists github.pulls (
  pull_number int comment "GitHub pull number",
  user_id string comment "GitHub user id",
  user_is_bot boolean comment "True if user is a bot",
  user_is_core boolean comment "True if user is part of GE core team", 
  base_ref string comment "target branch",
  head_ref string comment "source branch",
  created_ts timestamp comment "pull request created timestamp",
  merged_ts timestamp comment "pull request merged timestamp",
  closed_ts timestamp comment "pull request closed timestamp", 
  is_closed boolean comment "True if pull request has been closed"
) comment 'github pull requests in oss repo'
"""
spark.sql(sql)

#insert rows from spark tmp table
sql = """insert into github.pulls select * from pulls_table"""
spark.sql(sql)
