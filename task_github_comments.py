# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Comments

# COMMAND ----------

import repo_utilities as utl

# COMMAND ----------

# widget for temporal scope
dbutils.widgets.dropdown(
    name="scope", defaultValue="new only", choices=["new only", "all"]
)

# COMMAND ----------

# get data from api
if dbutils.widgets.get("scope") == "all":
    get_full_history = True
else:
    get_full_history = False

comments_history = utl.get_comments_history(get_full_history=get_full_history)

# COMMAND ----------

# put into a dataframe
df_comments = utl.get_comments_dataframe(comments_history)

# COMMAND ----------

# put into a tmp table
df_new = spark.createDataFrame(df_comments)
df_new.createOrReplaceTempView("new_comments")

# COMMAND ----------

# drop and rebuild or insert into existing hive table
if get_full_history:
    sql = """drop table if exists github.comments"""
    print("Dropping existing comments table and rebuilding.")
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
        ) comment 'GitHub comments in OSS repo'
    """
    spark.sql(sql)
    
    print("Inserting all comments into comments table.")
    sql = """insert into github.comments select distinct * from new_comments"""
    spark.sql(sql)
    
else:
    print("Inserting only new comments into existing comments table.")
    sql = """ 
        insert into github.comments
        select n.* from new_comments as n
        left join github.comments as o 
        on n.comment_id = o.comment_id
        where o.comment_id is null
        """
    spark.sql(sql)
