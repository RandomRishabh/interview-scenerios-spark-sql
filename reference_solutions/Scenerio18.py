# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio18").getOrCreate()

# Create input DataFrame
inputdf = spark.createDataFrame([("The Social Dilemma",)], ["word"])
inputdf.show()

# Define UDF for reversing words
def reverse_sentence(sentence):
    return " ".join([word[::-1] for word in sentence.split(" ")])

# Register UDF
reverse_udf = udf(reverse_sentence, StringType())

# Apply UDF to input DataFrame
outputdf = inputdf.withColumn("reverse word", reverse_udf("word")).drop("word")
outputdf.show()