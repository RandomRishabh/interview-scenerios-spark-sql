# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio14").getOrCreate()

data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(data, ["rollno", "name", "telugu", "english", "maths", "science", "social"])
df.show()

# Through SQL
df.createOrReplaceTempView("marks")
spark.sql("select *, (telugu+english+maths+science+social) as total from marks").show()

# Through DSL
finaldf = df.withColumn("total", expr("telugu+english+maths+science+social")).show()
