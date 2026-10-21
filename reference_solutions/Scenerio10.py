# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio10").getOrCreate()
data = [
    (1, 300, "31-Jan-2021"),
    (1, 400, "28-Feb-2021"),
    (1, 200, "31-Mar-2021"),
    (2, 1000, "31-Oct-2021"),
    (2, 900, "31-Dec-2021")
]
df = spark.createDataFrame(data, ["empid", "commissionamt", "monthlastdate"])
df.show()

maxdatedf = df.groupBy(col("empid").alias("empid1")).agg(max("monthlastdate").alias("maxdate"))
maxdatedf.show()

joindf = df.join(maxdatedf, (df["empid"] == maxdatedf["empid1"]) & (df["monthlastdate"] == maxdatedf["maxdate"]),
                 "inner").drop("empid1", "maxdate")
joindf.show()
