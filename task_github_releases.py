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
    version string comment "GX release version", 
    release_dt date comment "GX version release date",
    major int comment "GX version major release number <major.minor.patch>", 
    minor int comment "GX version minor release number <major.minor.patch>", 
    patch int comment "GX version patch release number <major.minor.patch>"
    ) comment 'GX OSS release history'
"""
spark.sql(sql)

sql = """
insert into github.releases 
select * from releases
"""
spark.sql(sql)
