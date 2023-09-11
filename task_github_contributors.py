# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Contributor

# COMMAND ----------

import repo_utilities as utl

# COMMAND ----------

# get contributor data from github api as json data
response_contributors = utl.send_request_to_github_api("contributors", True)

# COMMAND ----------

# parse contributor json data and dump into pandas df
df_contrib = utl.get_contributors_dataframe(response_contributors)

# COMMAND ----------

# get all-time contributor metrics as df
df_contrib_user = utl.get_contributors_users_dataframe(df_contrib)

# convert to spark / sql
df_spark = spark.createDataFrame(df_contrib_user)
df_spark.createOrReplaceTempView("users_table")

# COMMAND ----------

# always drop table (small # of rows; no need for incremental)
sql = """drop table if exists github.contributors"""
spark.sql(sql)

# schema
sql = """
create table if not exists github.contributors (
  user_id string comment "GitHub user id",
  user_name string comment "GitHub user name",
  user_is_core boolean comment "True if user is part of GE core team",
  adds int comment "total add events",
  deletes int comment "total delete events",
  commits int comment "total commit events",
  events int comment "total events (adds, deletes and commits)",
  first_week date comment "first week with >0 events",
  last_week date comment "last week with >0 events",
  active_weeks int comment "total weeks with >0 events",
  span_weeks int comment "weeks lapsed between user's first and last week"
) comment 'details about contributors to great-expectations oss repo'
"""
spark.sql(sql)

# insert rows from spark tmp table
sql = """insert into github.contributors select * from users_table"""
spark.sql(sql)

# COMMAND ----------

# get weekly contributor metrics
df_contrib_weekly = utl.get_contributors_weeks_dataframe(df_contrib, df_contrib_user)

# convert to spark / sql
df_spark = spark.createDataFrame(df_contrib_weekly)
df_spark.createOrReplaceTempView("weekly_users_table")

# COMMAND ----------

# always drop table (small # of rows; no need for incremental)
sql = """drop table if exists github.contributors_weekly_activity"""
spark.sql(sql)

# schema
sql = """
    create table if not exists github.contributors_weekly_activity (
    week date comment "week of contributor activity",
    user_id string comment "GitHub user id",
    adds int comment "count of add events in the week",
    deletes int comment "count of delete events in the week",
    commits int comment "count of commit events in the week",
    events int comment "count of any events (add, delete or commit) in the week"
    ) comment 'weekly summary of activity by github contributors to great-expectations oss repo'
    """
spark.sql(sql)

# insert rows from spark tmp table
sql = """insert into github.contributors_weekly_activity select * from weekly_users_table"""
spark.sql(sql)
