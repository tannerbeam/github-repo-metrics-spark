# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Releases

# COMMAND ----------

import repo_utilities as utl

# COMMAND ----------

release_history = utl.get_release_history()

# COMMAND ----------

releases_df = utl.get_releases_dataframe(release_history)

# COMMAND ----------

df_spark = spark.createDataFrame(releases_df)
df_spark.createOrReplaceTempView("releases")

# COMMAND ----------

sql = """drop table if exists github.releases"""
spark.sql(sql)

sql = """
create table if not exists github.releases (
    version str comment "GX release version", 
    release_dt date comment "GX version release date",
    is_yanked boolean comment "True if release was yanked",
    is_pre_release boolean comment "True if tagged as pre-release version.",
    major int comment "GX version major release number <major.minor.patch>", 
    minor int comment "GX version minor release number <major.minor.patch>", 
    patch int comment "GX version patch release number <major.minor.patch>", 
    pre_release str comment "GX pre-release version"
    ) comment 'GX OSS release history'
"""
spark.sql(sql)

sql = """
insert into github.releases 
select * from releases
"""
spark.sql(sql)
