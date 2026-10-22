# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio19").getOrCreate()

# Small JSON fixture from Datasets/scen.json; no external file upload needed.
json_text = '{\n  "code": 1234,\n  "commentCount": 5,\n  "createdAt": "2023-05-30T10:30:00",\n  "description": "Example description",\n  "feedsComment": "Example comment",\n  "id": 1,\n  "imagePaths": "/path/to/images",\n  "images": "image1.jpg,image2.jpg,image3.jpg",\n  "isdeleted": false,\n  "lat": 123456789,\n  "likeDislike": {\n    "dislikes": 10,\n    "likes": 20,\n    "userAction": 1\n  },\n  "lng": 987654321,\n  "location": "Example location",\n  "mediatype": 1,\n  "msg": "Example message",\n  "multiMedia": [\n    {\n      "createAt": "2023-05-30T12:00:00",\n      "description": "Media description",\n      "id": 1001,\n      "likeCount": 50,\n      "mediatype": 1,\n      "name": "Media name",\n      "place": "Media place",\n      "url": "https://example.com/media1"\n    },\n    {\n      "createAt": "2023-05-30T13:30:00",\n      "description": "Another media description",\n      "id": 1002,\n      "likeCount": 30,\n      "mediatype": 2,\n      "name": "Another media name",\n      "place": "Another media place",\n      "url": "https://example.com/media2"\n    }\n  ],\n  "name": "John Doe",\n  "profilePicture": "/path/to/profile_picture.jpg",\n  "title": "Example title",\n  "userId": 123,\n  "videoUrl": "https://example.com/video",\n  "totalFeed": 100\n}\n'
json_schema = schema_of_json(lit(json_text))
parseddf = spark.createDataFrame([(json_text,)], ["payload"]).select(
    from_json(col("payload"), json_schema).alias("record")
)
df = parseddf.select("record.*")
df.printSchema()
finaldf = df.withColumn("multiMedia", explode(col("multiMedia"))).withColumn("dislikes",
                                                                             expr("likeDislike.dislikes")).withColumn(
    "likes", expr("likeDislike.likes")).withColumn("userAction", expr("likeDislike.userAction")).withColumn("createAt",
                                                                                                            expr(
                                                                                                                "multiMedia.createAt")).withColumn(
    "description", expr("multiMedia.description")).withColumn("id", expr("multiMedia.id")).withColumn("likeCount", expr(
    "multiMedia.likeCount")).withColumn("mediatype", expr("multiMedia.mediatype")).withColumn("name", expr(
    "multiMedia.name")).withColumn("place", expr("multiMedia.place")).withColumn("url", expr("multiMedia.url")).drop(
    "likeDislike", "multiMedia")
print("flat Schema")
finaldf.printSchema()
finaldf.show()
