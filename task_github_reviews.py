# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Reviews

# COMMAND ----------

import repo_utilities as utl
import repo_init

env = repo_init.start()

# COMMAND ----------

dbutils.widgets.dropdown(
    name="scope", defaultValue="new only", choices=["new only", "all"]
)

# COMMAND ----------

if dbutils.widgets.get("scope") == "all":
    get_full_history = True
else:
    get_full_history = False

reviews_history = utl.get_reviews_history(get_full_history=get_full_history)

# COMMAND ----------

# put reviews into dataframe
df_reviews = utl.get_reviews_dataframe(reviews_history)

# COMMAND ----------

# convert to spark / sql
if not len(df_reviews) > 0:
    raise ValueError("No new reviews since last update.")

df_spark = spark.createDataFrame(df_reviews)
df_spark.createOrReplaceTempView("reviews_table")

# COMMAND ----------

if get_full_history:
    sql = """drop table if exists github.reviews"""
    print("Dropping reviews table to rebuild reviews history.")
    spark.sql(sql)
    sql = """
        create table if not exists github.reviews (
        review_id string comment "Github review id",
        pull_number int comment "GitHub pull number",
        user_id string comment "GitHub user id",
        user_name string comment "GitHub user login name",
        user_is_core boolean comment "True if user is part of GE core team",
        status string comment "review status",
        submitted_ts timestamp comment "review submitted timestamp"
        ) comment 'GitHub reviews in OSS repo'
    """
    spark.sql(sql)
else:
    print("Inserting new reviews into reviews table.")

# COMMAND ----------

# insert into table
sql = """insert into github.reviews select * from reviews_table"""
spark.sql(sql)
