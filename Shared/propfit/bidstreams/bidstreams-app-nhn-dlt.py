# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Bid Streams App NHN DLT
# MAGIC - Pipeline
# MAGIC   - EKS - MSK - Sink Connector - S3 - Databricks (Bronze - Gold)
# MAGIC - DLT
# MAGIC   - Bronze :: Raw
# MAGIC   - Gold :: Transformed & Parsed
# MAGIC - Topic
# MAGIC   - Bid
# MAGIC   - Clk
# MAGIC   - Imp
# MAGIC - Path Sample
# MAGIC   - `s3://ptbwa-basic/topics/streams_imp_app_nhn/22/10/28/15/15/`

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

import json
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Path with Asterisk

# COMMAND ----------

# YEAR/MONTH/DAY/HOUR/MINUTE
# s3://ptbwa-basic/topics/streams_imp_app_nhn/22/10/28/15/15/

# (!) syncing on every files on S3
bid_path = "dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=02/day=*/hour=*/minute=*"
bak_bid_path = "dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=02/day=*/hour=*/minute=*"

imp_path = "dbfs:/mnt/ptbwa-basic/topics/streams_imp_app_nhn/year=2023/month=02/day=*/hour=*/minute=*"
clk_path = "dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2023/month=02/day=*/hour=*/minute=*"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Schema
# MAGIC - Bid
# MAGIC   - `2022-11-24` V2 :: Add deviceType (OS|OSV)
# MAGIC   - `2022-12-15` V3 :: Change Schema to BidStreams Column
# MAGIC     - Add Request 
# MAGIC   - `2023-01-05` V4 :: Schema Evolution for Whole Fields
# MAGIC - Imp
# MAGIC - Clk
# MAGIC   - `2022-12-27` V2 :: Change Schema to BidStreams Column
# MAGIC     - Add show_date
# MAGIC     - Change win_price `Long => Double`
# MAGIC - Conv

# COMMAND ----------

with open("/dbfs/FileStore/configs/streams_bid_schema.json", "r") as f:
    schema = json.load(f)

# COMMAND ----------

bak_bid_schema = StructType.fromJson(json.loads(json.dumps(schema)))

# COMMAND ----------

# s3://ptbwa-basic/topics/streams_bid_app_nhn/year=2022/month=10/day=28/hour=15/minute=15/
# s3://ptbwa-basic/topics/streams_bid_app_nhn/year=2022/month=11/day=25/hour=17/minute=45/streams_bid_v2.json
# s3://ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=27/hour=20/minute=20/streams_clk_app_nhn+0+0000011712.json.gz

bid_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2022/month=11/day=25/hour=17/minute=45/streams_bid_v2.json")
imp_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_imp_app_nhn/year=2022/month=10/day=28/hour=15/minute=15/streams_imp_app_nhn+1+0000000000.json.gz")
clk_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=27/hour=20/minute=20/streams_clk_app_nhn+0+0000011712.json.gz")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Bronze Table
# MAGIC - Bid
# MAGIC - Imp
# MAGIC - Clk

# COMMAND ----------

@dlt.table
# @dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
# @dlt.expect_or_fail("valid_count", "click_count > 0")
def streams_bid_bronze_app_nhn():
    return (spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
#                 .option("cloudFiles.schemaLocation", "<path>")
#                 .option("cloudFiles.inferColumnTypes", "true")
#                 .option("mergeSchema", "true")
                .schema(bak_bid_schema)
                .load(bak_bid_path))

# COMMAND ----------

# @dlt.table
# def streams_bid_bronze_app_nhn_bak():
#     return (spark
#                 .readStream
#                 .format("cloudFiles")
#                 .option("cloudFiles.format", "json")
#                 .schema(bak_bid_schema)
#                 .load(bak_bid_path))

# COMMAND ----------

@dlt.table
def streams_imp_bronze_app_nhn():
    return (spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(imp_df.schema)
                .load(imp_path))

# COMMAND ----------

@dlt.table
def streams_clk_bronze_app_nhn():
    return (spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(clk_df.schema)
                .load(clk_path))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Remove Duplication
# MAGIC - Watermark
# MAGIC - in Every Streams

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Gold Table
# MAGIC - Bid
# MAGIC - Imp
# MAGIC - Clk

# COMMAND ----------

# @dlt.table
# @dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
# @dlt.expect_or_fail("valid_count", "click_count > 0")
# def tt_bidrequest_gold_app_nhn():
#     return (dlt
#             .read_stream("tt_bidrequest_bronze_app_nhn")
#             .select("tid", "actiontime_local", "request.app")) # (!) can access to nested structure

# COMMAND ----------

# @dlt.table
# def tt_bidrequest_gold_app_nhn():
#     return (dlt
#             .read_stream("tt_bidrequest_bronze_app_nhn")
#             .select("tid", "actiontime_local", "request.app")) # (!) can access to nested structure

# COMMAND ----------

# @dlt.table
# def tt_bidrequest_gold_app_nhn():
#     return (dlt
#             .read_stream("tt_bidrequest_bronze_app_nhn")
#             .select("tid", "actiontime_local", "request.app")) # (!) can access to nested structure

# COMMAND ----------

# @dlt.table
# def tt_bidrequest_gold_app_nhn():
#     return (dlt
#             .read_stream("tt_bidrequest_bronze_app_nhn")
#             .select("tid", "actiontime_local", "request.app")) # (!) can access to nested structure

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Request Column in Bidrequest App NHN
# MAGIC - neccessary define schema to extract in this column

# COMMAND ----------

# streaming_df = (spark
#                     .readStream
#                     .format("cloudFiles")
#                     .option("cloudFiles.format", "json")
#                     .schema(df.schema)
#                     .load(path))

# COMMAND ----------

# streaming_df.select("request.app").display()

# COMMAND ----------

# from pyspark.sql.functions import *
# df = spark.sql("select * from ice.streams_bid_bronze_app_nhn")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Change Data Type in Struct Field

# COMMAND ----------

# df = df.withColumn("BidStream", col("BidStream").withField("lat", col("BidStream.win_price").cast("double")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Overwrite Schema

# COMMAND ----------

# dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2022/month=12/day=17/hour=04/minute=15/streams_bid_app_nhn+0+0000714863.json.gz

# COMMAND ----------

# dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=01/day=05/hour=19/minute=00/streams_bid_app_nhn+3+0000347010.json.gz

# COMMAND ----------

# df = spark.read.format("json").load("dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=01/day=05/hour=19/minute=00/streams_bid_app_nhn+3+0000347010.json.gz")

# COMMAND ----------

# df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("ice.streams_bid_bronze_app_tmp")