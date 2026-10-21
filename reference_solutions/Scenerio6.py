# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio6").getOrCreate()
data = [
      ("1", "a", "10000"),
      ("2", "b", "5000"),
      ("3", "c", "15000"),
      ("4", "d", "25000"),
      ("5", "e", "50000"),
      ("6", "f", "7000")
]
myschema = ["empid","name","salary"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

#Through SQL
df.createOrReplaceTempView("emptab")
spark.sql("select *, case when salary > 10000 then 'Manager' else 'Employee' end as Designation from emptab").show()

#Through DSL
finaldf = df.withColumn("Desgination", expr("case when salary > 10000 then 'Manager' else 'Employee' end"))
finaldf.show()