# Databricks notebook source
# MAGIC %md
# MAGIC # GitHub OSS Metrics: Releases

# COMMAND ----------

import repo_utilities as utl
import repo_init

env = repo_init.start()

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

# duplicate to reference table for convenience
sql = """
drop table if exists reference.ge_versions
"""
spark.sql(sql)

sql = """
create table if not exists reference.ge_versions as 
select * from github.releases
"""
spark.sql(sql)
