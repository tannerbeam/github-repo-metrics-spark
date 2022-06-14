# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Comments

# COMMAND ----------

import repo_utilities as utl
import repo_init 
env = repo_init.start()

# COMMAND ----------

dbutils.widgets.dropdown(
    name = "scope", 
    defaultValue = "new only", 
    choices = ["new only", "all"]
)

# COMMAND ----------

# get comments history from github api

if dbutils.widgets.get("scope") == "all": 
    get_full_history = True 
else:
    get_full_history = False
    
comments_history = utl.get_comments_history(get_full_history = get_full_history)

# COMMAND ----------

# put comments history into a dataframe
df_comments = utl.get_comments_dataframe(comments_history)

if not len(df_comments) > 0:
    raise ValueError("No new comments since last update.")

#convert to spark / sql
df_spark = spark.createDataFrame(df_comments)
df_spark.createOrReplaceTempView("comments_table")

# COMMAND ----------

# drop existing if overwriting full history
if get_full_history: 
    sql = """drop table if exists github.comments"""
    print("Dropping comments table to rebuild comments history.")
    spark.sql(sql)
    sql = """
        create table if not exists github.comments (
        comment_id string comment "Github comment id", 
        issue_number int comment "GitHub issue number",
        user_id string comment "GitHub user id",
        user_name string comment "GitHub user login name",
        user_is_core boolean comment "True if user is part of GE core team",
        created_ts timestamp comment "issue created timestamp",
        updated_ts timestamp comment "issue updated timestamp",
        body string comment "comment body text"
        ) comment 'github comments in oss repo'
    """
    spark.sql(sql)
else:
    print("Inserting new comments into comments table.")

# COMMAND ----------

# insert into table
sql = """insert into github.comments select * from comments_table"""
spark.sql(sql)
