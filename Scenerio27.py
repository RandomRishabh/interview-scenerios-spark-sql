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
    spark = SparkSession.builder.master("local[2]").appName("Scenerio27").getOrCreate()

data = [(1,60000,2018),(1,70000,2019),(1,80000,2020),(2,60000,2018),(2,65000,2019),(2,65000,2020),(3,60000,2018),(3,65000,2019)]

df = spark.createDataFrame(data,["empid","salary","year"])

df.show()

wn = Window.partitionBy("empid").orderBy("year")

lagdf = df.withColumn("diff",lag("salary",1).over(wn))
lagdf.show()

finaldf = lagdf.withColumn("incresalary",expr("salary - diff")).drop("diff").na.fill(0).orderBy("empid","year")

finaldf.show()