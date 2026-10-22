# Databricks notebook source

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio12").getOrCreate()

#creating UDF functions for masked data, here email[0] is it will take first letter i.e 0th index and email[8:] is it will take the string from 8th index position to end of the string
def mask_email(email):
    return (email[0] + "**********" + email[8:])

#creating UDF functions for masked data, here mobile[0:2] is it will take string from Index 0 to 2 letters and mobile[-3:] is it will take string last three index to end the end of the string
def mask_mobile(mobile):
    return (mobile[0:2] + "*****" + mobile[-3:])


df = spark.createDataFrame([("Renuka1992@gmail.com", "9856765434"), ("anbu.arasu@gmail.com", "9844567788")], ["email", "mobile"])
df.show()

maskeddf = df.withColumn("email",udf(mask_email)(df.email)).withColumn("mobile",udf(mask_mobile)(df.mobile))
maskeddf.show()
