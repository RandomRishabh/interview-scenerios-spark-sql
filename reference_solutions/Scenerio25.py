# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio25").getOrCreate()

import csv
import io

# Small fixture from Datasets/Scenerio25.csv, including malformed rows.
csv_text = 'emp_no,emp_name,dep\n101,Murugan,HealthCare\nInvalid Entry,Description: Bad Record Entry\n102,Kannan,Finance\n103,Mani,IT\nConnection lost,Description: Poor Connection\n104,Pavan,HR\nBad Record,Description:Corrupt Record'
reader = csv.reader(io.StringIO(csv_text))
columns = next(reader)
# Reject rows with the wrong number of fields before making the DataFrame.
rows = [tuple(row) for row in reader if len(row) == len(columns)]
df = spark.createDataFrame(rows, schema=columns)
df.show()
