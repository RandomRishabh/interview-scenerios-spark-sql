# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Databricks supplies spark; create a local session only outside Databricks.
if "spark" not in globals():
    spark = SparkSession.builder.master("local[2]").appName("Scenerio20").getOrCreate()

# Small JSON fixture from Datasets/scen20.json; no external file upload needed.
json_text = '[\n  {\n    "code": 123,\n    "commentCount": 5,\n    "createAt": "2023-05-30T10:30:00",\n    "createdAt": "2023-05-30T10:30:00",\n    "description": "This is a sample description.",\n    "dislikes": 2,\n    "feedsComment": "Sample feeds comment",\n    "id": 1,\n    "imagePaths": "path/to/images",\n    "images": "image1.jpg,image2.jpg",\n    "isdeleted": false,\n    "lat": 12,\n    "likeCount": 10,\n    "likes": 8,\n    "lng": 34,\n    "location": "Sample location",\n    "mediatype": 1,\n    "msg": "Sample message",\n    "name": "John Doe",\n    "place": "Sample place",\n    "profilePicture": "path/to/profile_picture.jpg",\n    "title": "Sample title",\n    "totalFeed": 100,\n    "url": "http://sampleurl.com",\n    "userAction": 1,\n    "userId": 12345,\n    "videoUrl": "http://samplevideourl.com"\n  },\n  {\n    "code": 456,\n    "commentCount": 3,\n    "createAt": "2023-05-29T15:45:00",\n    "createdAt": "2023-05-29T15:45:00",\n    "description": "Another sample description.",\n    "dislikes": 1,\n    "feedsComment": "Another sample feeds comment",\n    "id": 2,\n    "imagePaths": "path/to/images2",\n    "images": "image3.jpg,image4.jpg",\n    "isdeleted": true,\n    "lat": 56,\n    "likeCount": 20,\n    "likes": 18,\n    "lng": 78,\n    "location": "Another sample location",\n    "mediatype": 2,\n    "msg": "Another sample message",\n    "name": "Jane Smith",\n    "place": "Another sample place",\n    "profilePicture": "path/to/profile_picture2.jpg",\n    "title": "Another sample title",\n    "totalFeed": 200,\n    "url": "http://anotherurl.com",\n    "userAction": 2,\n    "userId": 67890,\n    "videoUrl": "http://samplevideourl2.com"\n  }\n]\n'
json_schema = schema_of_json(lit(json_text))
parseddf = spark.createDataFrame([(json_text,)], ["payload"]).select(
    from_json(col("payload"), json_schema).alias("record")
)
df = parseddf.select(explode(col("record")).alias("record")).select("record.*")
df.printSchema()
compdf = df.select(
    col("code"),
    col("commentCount"),
    col("createdAt"),
    col("description"),
    col("feedsComment"),
    col("id"),
    col("imagePaths"),
    col("images"),
    col("isdeleted"),
    col("lat"),
    struct(col("dislikes"), col("likes"), col("userAction")).alias("likeDislike"),
    col("lng"),
    col("location"),
    col("mediatype"),
    col("msg"),
    array(
        struct(
            col("createAt"),
            col("description"),
            col("id"),
            col("likeCount"),
            col("mediatype"),
            col("name"),
            col("place"),
            col("url")
        ).alias("element")
    ).alias("multiMedia"),
    col("name"),
    col("profilePicture"),
    col("title"),
    col("userId"),
    col("videoUrl"),
    col("totalFeed")
)

compdf.printSchema()
