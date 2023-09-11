# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Reviews

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

reviews_history = utl.get_reviews_history(get_full_history=get_full_history)

# COMMAND ----------

# put into dataframe
df_reviews_new = utl.get_reviews_dataframe(reviews_history)

# COMMAND ----------

# put into a tmp table
df_new = spark.createDataFrame(df_reviews_new)
df_new.createOrReplaceTempView("new_reviews")

# COMMAND ----------

# drop and rebuild or insert into existing hive table
if get_full_history:    
    sql = """drop table if exists github.reviews"""
    print("Dropping existing reviews table and rebuilding.")
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
    
    print("Inserting all reviews into reviews table.")
    sql = """insert into github.reviews select distinct * from new_reviews"""
    spark.sql(sql)
    
else:
    print("Inserting only new reviews into existing reviews table.")
    sql = """ 
        insert into github.reviews
        select n.* from new_reviews as n
        left join github.reviews as o 
        on n.review_id = o.review_id
        where o.review_id is null
        """
    spark.sql(sql)
