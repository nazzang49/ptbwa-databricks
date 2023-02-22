# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Peoplecar Feature Dataset Test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install user-agents

# COMMAND ----------

import os
import json
import pprint

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from user_agents import parse

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Read Data
# MAGIC - ADID
# MAGIC - Propfit
# MAGIC - MMP
# MAGIC - Channel
# MAGIC   - `CM`

# COMMAND ----------

df = (spark
    .read
    .format("csv")
    .load("dbfs:/FileStore/propfit_pcar_adid.csv"))

# COMMAND ----------

df.createOrReplaceTempView("adid_view")

# COMMAND ----------

query = """
SELECT BidStream.*
FROM   ice.streams_imp_bronze_app_nhn
WHERE  actiontime_local >= '2022-12-17 11:00:00'
AND    actiontime_local <= '2022-12-19 22:00:00'
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

df.createOrReplaceTempView("imp_view")

# COMMAND ----------

query = """
SELECT *
FROM   ice.af_peoplecar_org
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

df.createOrReplaceTempView("af_view")

# COMMAND ----------

# query = """
# SELECT *
# FROM   auto_report.gad_pcar_adgroup_stats 
# """

# COMMAND ----------

# df = spark.sql(query)

# COMMAND ----------

# df.createOrReplaceTempView("gad_view")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Join

# COMMAND ----------

query = """
SELECT *
FROM   adid_view av
       JOIN imp_view iv
         ON av._c0 = iv.DeviceId
WHERE  av._c0 != '00000000-0000-0000-0000-000000000000' 
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

df.createOrReplaceTempView("first_join_view")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Second Join
# MAGIC - on Appsflyer Organization
# MAGIC   - IDFA :: AOS
# MAGIC   - IDFV :: IOS
# MAGIC   - Advertising_ID

# COMMAND ----------

query = """
SELECT *
FROM   first_join_view fjv
       JOIN af_view av
         ON fjv._c0 = av.IDFV
         OR fjv._c0 = av.IDFA
         OR fjv._c0 = av.Advertising_ID
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

df.createOrReplaceTempView("second_join_view")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### EDA
# MAGIC - 지역정보
# MAGIC - 장치정보
# MAGIC - 광고정보
# MAGIC - 어플정보

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from second_join_view

# COMMAND ----------

# (!) change into sql
def drop_null_columns(df):
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    
    # (!) check null counts of columns
    to_drop = [k for k, v in null_counts.items() if v >= df.count()]
    return df.drop(*to_drop)

# COMMAND ----------

df_dropped = drop_null_columns(df)

# COMMAND ----------

df_dropped.createOrReplaceTempView("dropped_second_join_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dropped_second_join_view

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc dropped_second_join_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Main Columns
# MAGIC - After Dropped Whole-Null Columns
# MAGIC - Distinct Required

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT DISTINCT bidrequest_id,
# MAGIC        tid,
# MAGIC        _c0,
# MAGIC        DeviceId,
# MAGIC        Advertising_ID,
# MAGIC        AudienceGroupInfoIdx,
# MAGIC        InventoryIdx,
# MAGIC        ad_grp,
# MAGIC        ad_imp,
# MAGIC        auc_id,
# MAGIC        bid_price,
# MAGIC        win_price,
# MAGIC        bundle_domain,
# MAGIC        App_ID,
# MAGIC        App_Name,
# MAGIC        App_Version,
# MAGIC        ga_cid,
# MAGIC        refUrl,
# MAGIC        w,
# MAGIC        h,
# MAGIC        City,
# MAGIC        Event_Time,
# MAGIC        Install_Time,
# MAGIC        state,
# MAGIC        User_Agent
# MAGIC FROM   dropped_second_join_view 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Parse UA

# COMMAND ----------

df = _sqldf

# COMMAND ----------

uas = df.select("User_Agent").collect()

# COMMAND ----------

tmp = [parse(ua[0]) for ua in uas]

# COMMAND ----------

# print(tmp[0].browser)
print(tmp[0].device)
# print(tmp[0].os)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Add BidRequest

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Add App Info

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save

# COMMAND ----------

df_dropped.write.mode("append").saveAsTable("ice.tt_imp_propfit_combine")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

