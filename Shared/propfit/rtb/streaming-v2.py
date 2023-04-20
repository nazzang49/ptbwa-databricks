# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Streams Raw Data Additional Pipeline
# MAGIC - Sources
# MAGIC   - bid
# MAGIC   - imp
# MAGIC   - clk
# MAGIC - Ref
# MAGIC   - [Archi on Wiki](https://ptbwa.atlassian.net/wiki/spaces/DS/pages/119177232/Archi)

# COMMAND ----------

!pip install ua-parser
!pip install user-agents

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from ua_parser import user_agent_parser
from user_agents import parse 

import json
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### UDF

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Parse User Agent
# MAGIC - (Ref) No Standard Packages

# COMMAND ----------

# ua_string = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 Safari/537.36'

# {   'device': {'brand': 'Apple', 'family': 'Mac', 'model': 'Mac'},
#     'os': {   'family': 'Mac OS X',
#               'major': '10',
#               'minor': '9',
#               'patch': '4',
#               'patch_minor': None},
#     'string': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) '
#               'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.104 '
#               'Safari/537.36',
#     'user_agent': {   'family': 'Chrome',
#                       'major': '41',
#                       'minor': '0',
#                       'patch': '2272'}}

# COMMAND ----------

def parse_user_agent(ua):
    """
    A method for parsing user_agent as UDF
    """
    
    parsed_string = parse(ua) if ua is not None else ""
    
    output =  [
        parsed_string.device.brand,
        parsed_string.device.family,
        parsed_string.device.model,
        parsed_string.os.family,
        parsed_string.os.version_string,
        parsed_string.browser.family,
        parsed_string.browser.version_string,
        (parsed_string.is_mobile or parsed_string.is_tablet),
        parsed_string.is_bot
    ]
    
    for i in range(len(output)):
        if output[i] is None:
            output[i] = 'Unknown' # NoneType Bypass
    return output

# COMMAND ----------

udf_parse_user_agent = udf(parse_user_agent, StructType([
    StructField("ua_device_brand", StringType(), True),
    StructField("ua_device_family", StringType(), True),
    StructField("ua_device_model", StringType(), True),
    StructField("ua_os_family", StringType(), True),
    StructField("ua_os_version", StringType(), True),
    StructField("ua_browser_family", StringType(), True),
    StructField("ua_browser_version", StringType(), True),
    StructField("ua_is_mobile", BooleanType(), True),
    StructField("ua_is_bot", BooleanType(), True),
])) 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Flatten Request in Bid
# MAGIC - App
# MAGIC   - Bundle
# MAGIC - Imp
# MAGIC   - Banner
# MAGIC - Device
# MAGIC   - Geo
# MAGIC   - IP
# MAGIC   - UA
# MAGIC   - OS
# MAGIC   - Model/Make

# COMMAND ----------

@dlt.table(
    comment="A table flattening streams_bid_bronze_app_nhn.BidStream.request",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
@dlt.expect_or_drop("valid_actiontime_date", "actiontime_date > '2023-02-01'")
def streams_bid_flatten_request():
    df = (
        spark
            .readStream
            .format("delta")
            .table("ice.streams_bid_bronze_app_nhn")
            .select(
                "BidStream.bidfloor",
                "BidStream.bid_price",
                "BidStream.request.*", 
                "BidStream.DeviceId", 
                "BidStream.ad_adv", 
                "BidStream.ad_cam", 
                "BidStream.ad_grp", 
                "BidStream.InventoryIdx", 
                "tid", 
                "actiontime_local", 
                "actiontime_date", 
                "actiontime_hour"
            )
            .withColumnRenamed("DeviceId", "device_id")
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Splits

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Bundle
# MAGIC - Domain
# MAGIC - Bid Price (PTBWA)
# MAGIC - Bid Floor (ADX)

# COMMAND ----------

@dlt.table(
    comment="A table selecting bundles in app",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_bundle():
    query = """
        SELECT app.bundle,
               bid_price,
               bidfloor as bid_floor,
               device_id, 
               ad_adv, 
               ad_cam, 
               ad_grp, 
               InventoryIdx, 
               tid, 
               actiontime_local, 
               actiontime_date, 
               actiontime_hour
        FROM   LIVE.streams_bid_flatten_request
    """
    
    return spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Banner
# MAGIC - Width 
# MAGIC - Height

# COMMAND ----------

@dlt.table(
    comment="A table selecting banners in imp",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_banner():
    query = """
        SELECT imp[0].banner.w as banner_w,
               imp[0].banner.h as banner_h,
               device_id, 
               ad_adv, 
               ad_cam, 
               ad_grp, 
               InventoryIdx, 
               tid, 
               actiontime_local, 
               actiontime_date, 
               actiontime_hour
        FROM   LIVE.streams_bid_flatten_request
    """
    
    return spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Device) Geo
# MAGIC - Country
# MAGIC - Region
# MAGIC - Lat/Lon
# MAGIC - With
# MAGIC   - IP

# COMMAND ----------

@dlt.table(
    comment="A table selecting GEOs in device",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_geo():
    query = """
        SELECT device.geo.country,
               device.geo.region,
               device.geo.lat, 
               device.geo.lon, 
               device.ip,
               ad_adv, 
               ad_cam, 
               ad_grp, 
               InventoryIdx, 
               tid, 
               actiontime_local, 
               actiontime_date, 
               actiontime_hour
        FROM   LIVE.streams_bid_flatten_request
    """
    
    return spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Device) User Agent
# MAGIC - Parsed by Package
# MAGIC - With
# MAGIC   - OS
# MAGIC   - Model
# MAGIC   - Make

# COMMAND ----------

@dlt.table(
    comment="A table selecting UAs in device",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_ua():
    query = """
        SELECT device.ua,
               device.os,
               device.osv,
               device.model,
               device.make,
               ad_adv, 
               ad_cam, 
               ad_grp, 
               InventoryIdx, 
               tid, 
               actiontime_local, 
               actiontime_date, 
               actiontime_hour
        FROM   LIVE.streams_bid_flatten_request
    """
    
    df = spark.sql(query)
    df = df.withColumn("parsed_ua", udf_parse_user_agent(col("ua")))
    
    return (
        df.select(
            "parsed_ua.*", 
            "os",
            "osv",
            "make",
            "model",
            "ad_adv",
            "ad_cam",
            "ad_grp",
            "InventoryIdx",
            "tid",
            "actiontime_local",
            "actiontime_date",
            "actiontime_hour"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Aggregation
# MAGIC - (Default) Count
# MAGIC - (Optional)
# MAGIC   - Sum
# MAGIC   - Avg
# MAGIC   - Min / Max
# MAGIC - `(Required) Group By Key`
# MAGIC   - ad_adv
# MAGIC   - ad_cam
# MAGIC   - ad_grp
# MAGIC   - InventoryIdx
# MAGIC   - actiontime_date
# MAGIC   - actiontime_hour
# MAGIC - `(Additional) Group By Key`
# MAGIC   - based on Each Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Bundle

# COMMAND ----------

@dlt.table(
    comment="A table counting bundles group by multiple keys",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_bundle_cnt():
    query = """
        SELECT *
        FROM   LIVE.streams_bid_request_bundle
    """
    
    groupby_cols = [
        "bundle",
        "ad_adv",
        "ad_cam",
        "ad_grp",
        "InventoryIdx",
        "actiontime_date",
        "actiontime_hour"
    ]
    
    df = (
        spark.sql(query)
            .withWatermark("actiontime_local", "1 hours")
            .dropDuplicates(["tid"])
    )
    
    return df.groupBy(*groupby_cols).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Banner

# COMMAND ----------

@dlt.table(
    comment="A table counting banners group by multiple keys",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_banner_cnt():
    query = """
        SELECT *
        FROM   LIVE.streams_bid_request_banner
    """
    
    groupby_cols = [
        "banner_w",
        "banner_h",
        "ad_adv",
        "ad_cam",
        "ad_grp",
        "InventoryIdx",
        "actiontime_date",
        "actiontime_hour"
    ]
    
    df = (
        spark.sql(query)
            .withWatermark("actiontime_local", "1 hours")
            .dropDuplicates(["tid"])
    )
    
    return df.groupBy(*groupby_cols).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Device) Geo

# COMMAND ----------

@dlt.table(
    comment="A table counting GEOs group by multiple keys",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_geo_cnt():
    query = """
        SELECT *
        FROM   LIVE.streams_bid_request_geo
    """
    
    groupby_cols = [
        "country",
        "region",
        "lat",
        "lon",
        "ip",
        "ad_adv",
        "ad_cam",
        "ad_grp",
        "InventoryIdx",
        "actiontime_date",
        "actiontime_hour"
    ]
    
    df = (
        spark.sql(query)
            .withWatermark("actiontime_local", "1 hours")
            .dropDuplicates(["tid"])
    )
    
    return df.groupBy(*groupby_cols).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Device) User Agent
# MAGIC - Groupby using parsed UA not OpenRTB Spec

# COMMAND ----------

@dlt.table(
    comment="A table counting UAs group by multiple keys",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
def streams_bid_request_ua_cnt():
    query = """
        SELECT *
        FROM   LIVE.streams_bid_request_ua
    """
    
    groupby_cols = [
        "ua_device_family",
        "ua_os_family",
        "ua_os_version",
        "ua_browser_family",
        "ua_browser_version",
        "ua_is_mobile",
        "ad_adv",
        "ad_cam",
        "ad_grp",
        "InventoryIdx",
        "actiontime_date",
        "actiontime_hour"
    ]
    
    df = (
        spark.sql(query)
            .withWatermark("actiontime_local", "1 hours")
            .dropDuplicates(["tid"])
    )
    
    return df.groupBy(*groupby_cols).count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Only Fraud Data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Device) UA Lang + OS + Emulator
# MAGIC - Condition
# MAGIC   - not like `lang:ko`
# MAGIC   - not like `android` and `ios`
# MAGIC   - like `sdk` and `x86`
# MAGIC - Mapping & Preprocessing on `Same Meaning but Different Notation`
# MAGIC   - e.g. `Android = aos`

# COMMAND ----------

filtering_ruleset = {
    "fraud_os": "lower(device.os) not like '%android%' and lower(device.os) not like '%ios%'",
    "fraud_ua_lang": "device.ua like '%lang:%' and device.ua not like '%lang:ko%'",
    "fraud_emulator": "device.ua like '%sdk%' and device.ua like '%x86%'",
}

# COMMAND ----------

@dlt.table(
    comment="A table selecting other languages in UAs",
    table_properties={
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=[
        "actiontime_date",
        "actiontime_hour"
    ]
)
@dlt.expect_or_all_drop(filtering_ruleset)
def streams_bid_request_fraud_ua_lang():
    query = """
        SELECT *
        FROM   LIVE.streams_bid_flatten_request
    """
    
#     df = (
#         spark.sql(query)
#             .withWatermark("actiontime_local", "1 hours")
#             .dropDuplicates(["tid"])
#     )
    
    return spark.sql(query)

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