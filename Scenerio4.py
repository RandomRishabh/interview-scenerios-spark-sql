# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio4").getOrCreate()
data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

# Through SQL
df.createOrReplaceTempView("custtab")

spark.sql(
    "select custid,custname,collect_set(address) as address from custtab group by custid,custname order by custid").show()

# Through DSL
finaldf = df.groupBy("custid", "custname").agg(collect_set("address").alias("address")).orderBy("custid").show()
