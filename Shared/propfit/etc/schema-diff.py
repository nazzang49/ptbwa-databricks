# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Create Propfit Schema
# MAGIC - Create BidRequest Schema
# MAGIC   - for AWS Glue
# MAGIC   - for Bid in Databricks
# MAGIC - `2023-01-05` V1 :: @jinyoung.park

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import pprint
import json

# COMMAND ----------

# dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=01/day=05/hour=20/minute=20/streams_bid_app_nhn+5+0000358986.json.gz

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Schema as JSON

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=01/day=05/hour=20/minute=20/streams_bid_app_nhn+5+0000358986.json.gz")

# COMMAND ----------

df.display()

# COMMAND ----------

pprint.pprint(df.schema.jsonValue())

# COMMAND ----------

bid_schema = df.schema.jsonValue()

# COMMAND ----------

bid_schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Schema

# COMMAND ----------

with open("/dbfs/FileStore/configs/streams_bid_schema.json", "r") as f:
    schema = json.load(f)

# COMMAND ----------

# schema

# COMMAND ----------

schema = StructType.fromJson(json.loads(json.dumps(schema)))

# COMMAND ----------

schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cast Previous Table

# COMMAND ----------

df = spark.read.schema(schema).json("dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=01/day=05/hour=20/minute=20/streams_bid_app_nhn+5+0000358986.json.gz")

# COMMAND ----------

df = spark.sql("select * from ice.streams_bid_bronze_app_nhn limit 1")

# COMMAND ----------

# df = df.withColumn(
#     "BidStream", col("BidStream").withField(
#         "request.device.geo.lat", col("BidStream.request.device.geo.lat").cast("double")
#     )
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Change Data Type on Struct Type

# COMMAND ----------

df = df.withColumn("BidStream", col("BidStream").withField("request.device.geo.lat", col("BidStream.request.device.geo.lat").cast("double")))

# COMMAND ----------

df = df.withColumn("BidStream", col("BidStream").withField("request.device.geo.lon", col("BidStream.request.device.geo.lon").cast("double")))

# COMMAND ----------

df = df.withColumn("BidStream", col("BidStream").withField("request.device.pxratio", col("BidStream.request.device.pxratio").cast("double")))

# COMMAND ----------

# =========================== HOW TO ARRAY STRUCT?


df_org = spark.read.option("mergeSchema", "true").format("delta").load("dbfs:/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/tables/streams_bid_bronze_app_nhn")

# COMMAND ----------

# df = df.withColumn("BidStream", col("BidStream").withField("request.array<imp>.banner.btype", col("BidStream.request.array<imp>.banner.btype").cast("array<string>")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Overwrite or Append with New Schema

# COMMAND ----------

# df_org.write.format("delta").mode("append").saveAsTable("ice.streams_bid_bronze_app_nhn_tmp")

# COMMAND ----------

# !cat /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/checkpoints/streams_bid_bronze_app_nhn/0/sources/0/rocksdb/logs/051566-65da8482-69f7-4331-a0ae-0ffb1f3fe14f.log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Quick Test

# COMMAND ----------

# s3://ptbwa-basic/test/streams_bid_app_nhn+5+0021951479.json
# df = spark.read.schema(bak_bid_schema).json("dbfs:/mnt/ptbwa-basic/test/streams_bid_app_nhn+5+0021951479.json")

# COMMAND ----------

# with open("/dbfs/FileStore/configs/streams_bid_schema.json", "r") as f:
#     schema = json.load(f)

# COMMAND ----------

# from pyspark.sql.types import *

# bak_bid_schema = StructType.fromJson(json.loads(json.dumps(schema)))

# COMMAND ----------

# df.write.mode("append").format("delta").saveAsTable("ice.tt_streams_bid_bronze_app_nhn")