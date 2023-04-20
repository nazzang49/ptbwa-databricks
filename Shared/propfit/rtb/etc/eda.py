# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

import json
import os

# COMMAND ----------

# query = """
# SELECT 
# 	BidStream.*
# FROM hive_metastore.ice.streams_imp_bronze_app_nhn 
# WHERE BidStream.DeviceId != '00000000-0000-0000-0000-000000000000'
# """

query = """
select Bidstream.* from ice.imp_tmp_221215
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

# df = spark.sql("select * from ice.imp_tmp")

# COMMAND ----------

# df.write.format("delta").mode("append").saveAsTable("ice.imp_tmp")

# COMMAND ----------

df.display()

# COMMAND ----------

query = """
SELECT *
FROM   ice.imp_tmp
WHERE  deviceid = '12b33f50-3d3d-4410-9a25-8e6c1cf94f0c' 
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Bundle Domain

# COMMAND ----------

freq_bundle_domains = [
    "com.miniram.donpush.one",
    "kr.co.angtalk.one",
    "com.hong.fo4book",
    "org.HanpanGo",
    "com.dcinside.app",
    "com.freeapp.androidapp",
    "com.fun.games.real.dream.football.league",
    "com.ktcs.whowho",
    "com.never.die",
    "oatp.co.kr.appletree",
]

# COMMAND ----------

filter_str = "','".join(freq_bundle_domains)

# COMMAND ----------

filter_str

# COMMAND ----------

query = f"""
select * from ice.imp_tmp where bundle_domain in ('{filter_str}')
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

df.createOrReplaceTempView("imp_tmp_view_221215")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select bundle_domain, count(bundle_domain) as cnt_domain
# MAGIC from imp_tmp_view_221215
# MAGIC group by bundle_domain
# MAGIC order by cnt_domain desc
# MAGIC limit 10

# COMMAND ----------

top_bundle_domains = _sqldf.collect()

# COMMAND ----------

top_bundle_domains

# COMMAND ----------

spark.sql("select win_price from imp_tmp_view_221215 where win_price = 0").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Win Price
# MAGIC - 1M Row 기준, Top Bundle Domain 추출
# MAGIC   - from Quick Dashboard Randomly
# MAGIC - 해당 Top Bundle Domain 포함 Row 조회
# MAGIC   - `3.8M`
# MAGIC - 87%
# MAGIC   - `0`
# MAGIC - 13%
# MAGIC   - avg
# MAGIC     - `0.0001`
# MAGIC   - cnt
# MAGIC     - `0.5M`
# MAGIC   - sum
# MAGIC     - `52`

# COMMAND ----------

# AudienceGroupInfoIdx

# COMMAND ----------

# bid_price

# COMMAND ----------

spark.sql("select sum(bid_price), avg(bid_price), count(bid_price) from imp_tmp_view").display()

# COMMAND ----------

query = """
SELECT bundle_domain,
       Sum(bid_price),
       Avg(bid_price),
       Count(bid_price),
       Sum(win_price),
       Avg(win_price),
       Sum(win_price) * 1000,
       Avg(win_price) * 1000
FROM   imp_tmp_view
GROUP  BY bundle_domain
"""

# COMMAND ----------

spark.sql(query).display()

# COMMAND ----------

# com.freeapp.androidapp => bid_price vs win_price diff big!

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*), sum(win_price)
# MAGIC from imp_tmp_view 
# MAGIC where bundle_domain = 'com.freeapp.androidapp'
# MAGIC and win_price = 0
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC select count(*), sum(win_price)
# MAGIC from imp_tmp_view 
# MAGIC where bundle_domain = 'com.freeapp.androidapp'
# MAGIC and win_price != 0

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct InventoryIdx
# MAGIC from imp_tmp_view 
# MAGIC where bundle_domain = 'com.freeapp.androidapp'
# MAGIC and win_price = 0
# MAGIC 
# MAGIC 
# MAGIC -- 146, 148 => mssql 비교 필요

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from hive_metastore.ice.cm_inventory
# MAGIC where InventoryIdx = 146

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc imp_tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Audience Group
# MAGIC - Required `cm_audiencegroupinfo`

# COMMAND ----------

query = f"""
select AudienceGroupInfoIdx, count(*) from imp_tmp_view group by AudienceGroupInfoIdx
"""

# COMMAND ----------

spark.sql(query).display()

# COMMAND ----------

import pickle

with open(f"/dbfs/FileStore/configs/mssql_live_config_new_test.pickle", "rb") as f:
    config = pickle.load(f)

# COMMAND ----------

config

# COMMAND ----------

push_down_query = "(select * from dbo.impressionHistory where InventoryIdx = 146) as impressionHistory"

# COMMAND ----------

impressionhistory_df = (spark
              .read
              .format("jdbc")
              .option("url", config["mssql_url"])
              .option("dbtable", push_down_query)
              .option("user", config["username"])
              .option("password", config["password"])
              .load())

impressionhistory_df = impressionhistory_df.withColumn("del_yn", col("del_yn").cast("int"))

# COMMAND ----------

impressionhistory_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### TID Duplication Check
# MAGIC - `2022-12-15`
# MAGIC   - 01GMAZJ85A5QBSD0HHKVJN6DSH
# MAGIC     - count 3
# MAGIC - `2022-12-19`
# MAGIC   - 01GMKXBDE3CS3Y2M2JH5X01CW3
# MAGIC     - count 3

# COMMAND ----------

query = """
SELECT BidStream.*
FROM   hive_metastore.ice.streams_imp_bronze_app_nhn
WHERE  actiontime_local >= '2022-12-19 00:00:00'
       AND tid = '01GMMFFH29K4PS29E0YZGKGDK8-1' 
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("tid_dup_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct DeviceId, bidrequest_id, ad_imp, bundle_domain, rawUrl from tid_dup_view

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

