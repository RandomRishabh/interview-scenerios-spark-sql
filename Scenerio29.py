# Databricks notebook source

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *


# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio29").getOrCreate()

data1 = [(1,), (2,), (3,)]

df1 = spark.createDataFrame(data1, ["col"])
df1.show()

data2 = [(1,), (2,), (3,), (4,), (5,)]

df2 = spark.createDataFrame(data2, ["col1"])
df2.show()

maxdf = df1.agg(max("col").alias("max"))
maxdf.show()

maxsalary = maxdf.select(col("max")).first()[0]

joindf = df1.join(df2, df1["col"] == df2["col1"], "outer").drop("col")
joindf.show()

finaldf = joindf.filter(col("col1") != maxsalary).withColumnRenamed("col1", "col").orderBy("col")
finaldf.show()
