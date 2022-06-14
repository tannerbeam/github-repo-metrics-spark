from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import os
import re
import sys


def start():
    """
    utility function for initializing a databricks notebook in /Workspace/Repos/
    """

    utils_path = "/Workspace/Repos/prod/databricks-utilities"
    if not utils_path in sys.path:
        sys.path.append(utils_path)

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    dbutils.widgets.removeAll()
    pat = r".*\/(dev|prod)\/([\w\-\_]+)"
    env = re.search(pat, os.getcwd()).group(1)
    repo = re.search(pat, os.getcwd()).group(2)

    wd = f"/Workspace/Repos/{env}/{repo}"
    os.chdir(wd)
    print(f"env: {env}")
    print(f"working directory: {wd}")

    return env
