# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Bid Streams App NHN DLT V2
# MAGIC - Advanced Spark-Streaming `@jinyoung.park`
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

# (!) syncing on every files on S3
bid_path = "dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2023/month=02|03|04|05|06|07|08/day=27/hour=17/minute=*"
imp_path = "dbfs:/mnt/ptbwa-basic/topics/streams_imp_app_nhn/year=2023/month=02|03|04|05|06|07|08/day=27/hour=17/minute=*"
clk_path = "dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2023/month=02|03|04|05|06|07|08/day=27/hour=17/minute=*"

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

# COMMAND ----------

with open("/dbfs/FileStore/configs/streams_bid_schema.json", "r") as f:
    schema = json.load(f)

# COMMAND ----------

bid_schema = StructType.fromJson(json.loads(json.dumps(schema)))

# COMMAND ----------

# bid_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_bid_app_nhn/year=2022/month=11/day=25/hour=17/minute=45/streams_bid_v2.json")
imp_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_imp_app_nhn/year=2022/month=10/day=28/hour=15/minute=15/streams_imp_app_nhn+1+0000000000.json.gz")
clk_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=27/hour=20/minute=20/streams_clk_app_nhn+0+0000011712.json.gz")

# COMMAND ----------

imp_df = imp_df.withColumn("BidStream", col("BidStream").withField("win_price", col("BidStream.win_price").cast("decimal(13, 10)")))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Real Time Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Sample

# COMMAND ----------

# from pyspark.sql.functions import expr

# impressions = spark.readStream. ...
# clicks = spark.readStream. ...

# # Apply watermarks on event-time columns
# impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
# clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

# # Join with event-time constraints
# impressionsWithWatermark.join(
#   clicksWithWatermark,
#   expr("""
#     clickAdId = impressionAdId AND
#     clickTime >= impressionTime AND
#     clickTime <= impressionTime + interval 1 hour
#     """)
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Bronze
# MAGIC - Raw

# COMMAND ----------

@dlt.table
def tt_streams_bid_bronze_app_nhn():
    bid_streams = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(bid_schema)
        .load(bid_path)) 
    return bid_streams

# COMMAND ----------

@dlt.table
def tt_streams_imp_bronze_app_nhn():
    imp_streams = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(imp_df.schema)
        .load(imp_path))
    return imp_streams

# COMMAND ----------

@dlt.table
def tt_streams_clk_bronze_app_nhn():
    clk_streams = (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(clk_df.schema)
        .load(clk_path))
    return clk_streams

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Silver
# MAGIC - Select Columns

# COMMAND ----------

@dlt.table
def tt_streams_bid_silver_app_nhn():
    query = f"""
        SELECT
            DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
            actiontime_local AS bidtime,
            BidStream.AdExchangeTypes AS AdExchangeTypes,
            BidStream.ad_adv AS ad_adv,
            BidStream.ad_imp AS PubId,
            BidStream.ad_grp AS ad_grp,
            BidStream.w AS Width,
            BidStream.h AS Height,
            BidStream.ad_crt AS ad_crt,
            BidStream.bundle_domain AS url,
            BidStream.InventoryIdx AS InventoryIdx,
            BidStream.ad_cam AS ad_cam,
            BidStream.UserId AS UserId,
            BidStream.DeviceId AS DeviceId,
            tid
        FROM LIVE.tt_streams_bid_bronze_app_nhn
    """
    
    # (!) deduplication with watermark
    deduplicated_bids = (spark
         .sql(query)
         .withWatermark("actiontime_local", "1 hours")
         .dropDuplicates(["tid"]))
    
    return deduplicated_bids

# COMMAND ----------

@dlt.table
def tt_streams_imp_silver_app_nhn():
    query = f"""
        SELECT
            DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
            actiontime_local AS imptime,
            BidStream.AdExchangeTypes AS AdExchangeTypes,
            BidStream.ad_adv AS ad_adv,
            BidStream.ad_imp AS PubId,
            BidStream.ad_grp AS ad_grp,
            BidStream.w AS Width,
            BidStream.h AS Height,
            BidStream.ad_crt AS ad_crt,
            BidStream.bundle_domain AS url,
            BidStream.InventoryIdx AS InventoryIdx,
            BidStream.ad_cam AS ad_cam,
            BidStream.UserId AS UserId,
            BidStream.DeviceId AS DeviceId,
            CAST(BidStream.win_price as DECIMAL(13, 10)) AS Revenue,
            tid
        FROM LIVE.tt_streams_imp_bronze_app_nhn
    """
    
    # (!) deduplication with watermark
    deduplicated_imps = (spark
         .sql(query)
         .withWatermark("actiontime_local", "2 hours")
         .dropDuplicates(["tid"]))
    
    return deduplicated_imps

# COMMAND ----------

@dlt.table
def tt_streams_clk_silver_app_nhn():
    query = f"""
        SELECT
            DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
            actiontime_local AS clktime,
            BidStream.AdExchangeTypes AS AdExchangeTypes,
            BidStream.ad_adv AS ad_adv,
            BidStream.ad_imp AS PubId,
            BidStream.ad_grp AS ad_grp,
            BidStream.w AS Width,
            BidStream.h AS Height,
            BidStream.ad_crt AS ad_crt,
            BidStream.bundle_domain AS url,
            BidStream.InventoryIdx AS InventoryIdx,
            BidStream.ad_cam AS ad_cam,
            BidStream.UserId AS UserId,
            BidStream.DeviceId AS DeviceId,
            tid
        FROM LIVE.tt_streams_clk_bronze_app_nhn
    """
    
    # (!) deduplication with watermark
    deduplicated_clks = (spark
         .sql(query)
         .withWatermark("actiontime_local", "3 hours")
         .dropDuplicates(["tid"]))
    
    return deduplicated_clks

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Gold
# MAGIC - Real Time Stat

# COMMAND ----------

# @dlt.table
# def tt_streams_real_time_stat():
    
#     query = f"""
#         SELECT
#             bid.Date,            -- bid_date
#             bid.bidtime,         -- bid_time
#             imp.imptime,         -- imp_time
#             clk.clktime,         -- clk_time
#             bid.AdExchangeTypes, -- cm_adx_idx
#             bid.ad_adv,          -- cm_advertiser_idx
#             bid.PubId,           -- imp_page_id
#             bid.ad_grp,          -- cm_adgroup_idx
#             bid.Width,           -- banner_width
#             bid.Height,          -- banner_height
#             bid.ad_crt,          -- ad_content
#             bid.url,             -- bundle_domain
#             bid.InventoryIdx,    -- cm_inventory_idx
#             bid.ad_cam,          -- cm_campaign_idx
#             bid.UserId,          -- adx_user_id
#             bid.DeviceId,        -- ad_id
#             bid.tid,
#             imp.Revenue AS Revenue,                         -- cost
#             TIMESTAMPDIFF(SECOND, imptime, clktime) AS ITCT -- duration_imp_to_clk
#         FROM LIVE.tt_streams_bid_silver_app_nhn AS bid
#         LEFT OUTER JOIN LIVE.tt_streams_imp_silver_app_nhn AS imp
#             ON bid.ad_adv = imp.ad_adv
#             AND bid.AdExchangeTypes = imp.AdExchangeTypes
#             AND bid.PubId = imp.PubId
#             AND bid.ad_grp = imp.ad_grp
#             AND bid.Width = imp.Width
#             AND bid.Height = imp.Height
#             AND bid.ad_crt = imp.ad_crt
#             AND bid.url = imp.url
#             AND bid.InventoryIdx = imp.InventoryIdx
#             AND bid.ad_cam = imp.ad_cam
#             AND bid.UserId = imp.UserId
#             AND bid.DeviceId = imp.DeviceId
#             AND bid.tid = imp.tid
#             AND imp.imptime >= bid.bidtime                        -- additional condition based on watermark
#             AND imp.imptime <= bid.bidtime + interval 1 hour      -- additional condition based on watermark
#         LEFT OUTER JOIN LIVE.tt_streams_clk_silver_app_nhn AS clk
#             ON bid.ad_adv = clk.ad_adv
#             AND bid.AdExchangeTypes = clk.AdExchangeTypes
#             AND bid.PubId = clk.PubId
#             AND bid.ad_grp = clk.ad_grp
#             AND bid.Width = clk.Width
#             AND bid.Height = clk.Height
#             AND bid.ad_crt = clk.ad_crt
#             AND bid.url = clk.url
#             AND bid.InventoryIdx = clk.InventoryIdx
#             AND bid.ad_cam = clk.ad_cam
#             AND bid.UserId = clk.UserId
#             AND bid.DeviceId = clk.DeviceId
#             AND bid.tid = clk.tid
#             AND clk.clktime >= imp.imptime                        -- additional condition based on watermark
#             AND clk.clktime <= imp.imptime + interval 1 hour      -- additional condition based on watermark
#         WHERE 1=1
#     """
#     return spark.sql(query)

# COMMAND ----------

@dlt.table
def tt_streams_real_time_stat():
    
    query = f"""
        SELECT
            bid.Date,            -- bid_date
            bid.bidtime,         -- bid_time
            imp.imptime,         -- imp_time
            clk.clktime,         -- clk_time
            bid.AdExchangeTypes, -- cm_adx_idx
            bid.ad_adv,          -- cm_advertiser_idx
            bid.PubId,           -- imp_page_id
            bid.ad_grp,          -- cm_adgroup_idx
            bid.Width,           -- banner_width
            bid.Height,          -- banner_height
            bid.ad_crt,          -- ad_content
            bid.url,             -- bundle_domain
            bid.InventoryIdx,    -- cm_inventory_idx
            bid.ad_cam,          -- cm_campaign_idx
            bid.UserId,          -- adx_user_id
            bid.DeviceId,        -- ad_id
            bid.tid,
            imp.Revenue AS Revenue,                         -- cost
            TIMESTAMPDIFF(SECOND, imptime, clktime) AS ITCT -- duration_imp_to_clk
        FROM LIVE.tt_streams_bid_silver_app_nhn AS bid
        LEFT OUTER JOIN LIVE.tt_streams_imp_silver_app_nhn AS imp
            ON bid.ad_adv = imp.ad_adv
            AND bid.AdExchangeTypes = imp.AdExchangeTypes
            AND bid.PubId = imp.PubId
            AND bid.ad_grp = imp.ad_grp
            AND bid.Width = imp.Width
            AND bid.Height = imp.Height
            AND bid.ad_crt = imp.ad_crt
            AND bid.url = imp.url
            AND bid.InventoryIdx = imp.InventoryIdx
            AND bid.ad_cam = imp.ad_cam
            AND bid.UserId = imp.UserId
            AND bid.DeviceId = imp.DeviceId
            AND bid.tid = imp.tid
        LEFT OUTER JOIN LIVE.tt_streams_clk_silver_app_nhn AS clk
            ON bid.ad_adv = clk.ad_adv
            AND bid.AdExchangeTypes = clk.AdExchangeTypes
            AND bid.PubId = clk.PubId
            AND bid.ad_grp = clk.ad_grp
            AND bid.Width = clk.Width
            AND bid.Height = clk.Height
            AND bid.ad_crt = clk.ad_crt
            AND bid.url = clk.url
            AND bid.InventoryIdx = clk.InventoryIdx
            AND bid.ad_cam = clk.ad_cam
            AND bid.UserId = clk.UserId
            AND bid.DeviceId = clk.DeviceId
            AND bid.tid = clk.tid
        WHERE 1=1
    """
    return spark.sql(query)

# COMMAND ----------

@dlt.table
def tt_streams_stat_gold():
    query = f"""
        SELECT
            Date,
            AdExchangeTypes AS ExchangeTypes,
            ad_adv,
            PubId,
            Width,
            Height,
            url,
            ad_grp,
            InventoryIdx,
            ad_cam,
            NVL(COUNT(bidtime), 0) AS bid,
            NVL(COUNT(imptime), 0) AS imp,
            NVL(COUNT(clktime), 0) AS clk,
            NVL(SUM(Revenue), 0) AS Revenue
        FROM LIVE.tt_streams_real_time_stat
        GROUP BY PubId, url, ad_cam, ad_grp, Width, Height, InventoryIdx, Date, AdExchangeTypes, ad_adv
    """
    return spark.sql(query)

# COMMAND ----------



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