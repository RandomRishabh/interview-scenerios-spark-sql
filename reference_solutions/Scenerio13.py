# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio13").getOrCreate()

data = [(1, "Jhon", "Development"),
        (2, "Tim", "Development"),
        (3, "David", "Testing"),
        (4, "Sam", "Testing"),
        (5, "Green", "Testing"),
        (6, "Miller", "Production"),
        (7, "Brevis", "Production"),
        (8, "Warner", "Production"),
        (9, "Salt", "Production")]
df = spark.createDataFrame(data, ["emp_id", "emp_name", "dept"])
df.show()

# Through SQL
df.createOrReplaceTempView("emptab")
spark.sql("SELECT dept, COUNT(*) AS total FROM emptab GROUP BY dept").show()

# Through DSL
finaldf = df.groupBy(col("dept")).agg(count("*").alias("total")).show()
