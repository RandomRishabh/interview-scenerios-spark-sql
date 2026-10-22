# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio23").getOrCreate()

data = [(1, 5), (2, 6), (3, 5), (3, 6), (1, 6)]
df = spark.createDataFrame(data, ["customer_id", "product_key"])
df.show()
data2 = [(5,), (6,)]
df2 = spark.createDataFrame(data2, ["product_key"])
df2.show()
finaldf = df.join(df2, ["product_key"], "inner").drop("product_key").distinct().filter(col("customer_id") != 2)
finaldf.show()
