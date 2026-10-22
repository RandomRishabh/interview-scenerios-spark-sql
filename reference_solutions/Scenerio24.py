# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio24").getOrCreate()

data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")]
df = spark.createDataFrame(data, ["userid", "page"])
df.show()
# Through SQL
df.createOrReplaceTempView("pagetab")
spark.sql("select userid, collect_list(page) as pages from pagetab group by userid").show()

# Through DSL
finaldf = df.groupBy("userid").agg(collect_list("page").alias("pages"))
finaldf.show(truncate=False)
