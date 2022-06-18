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

sql = """drop table if exists reference.ge_versions"""
spark.sql(sql)

sql = """
create table reference.ge_versions as
select * from releases
"""
spark.sql(sql)
