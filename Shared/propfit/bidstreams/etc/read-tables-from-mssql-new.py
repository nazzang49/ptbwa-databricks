# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #Read Tables from MSSQL
# MAGIC - Inventory
# MAGIC - Campaign
# MAGIC - AdGroup
# MAGIC - AdGroupSchedule

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
mssql_log = log4jLogger.LogManager.getLogger("READ_MSSQL")

# COMMAND ----------

!pip install attrdict

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Packages

# COMMAND ----------

from attrdict import AttrDict
from datetime import datetime, timedelta
from pyspark.sql.functions import *

import pickle
import re

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Class Definition

# COMMAND ----------

class MssqlWorker:
    
    def __init__(self, args) -> None:
        self.args = args
        self.args.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d") # e.g. 2022-06-02 (today)
        print(self.args.today)
    
    def read_inventory_from_mssql(self):
        try:
            self.inventory_df = (spark
              .read
              .format("jdbc")
              .option("url", self.args.mssql_url)
              .option("dbtable", self.args.tables.inventory)
              .option("user", self.args.username)
              .option("password", self.args.password)
              .load()
            )
            self.inventory_df = self.inventory_df.withColumn("DelYN", col("DelYN").cast("int"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-INVENTORY-FROM-MSSQL]{self.args}")
            raise e
    
    def read_adgroup_from_mssql(self):
        try:
            self.adgroup_df = (spark
                  .read
                  .format("jdbc")
                  .option("url", self.args.mssql_url)
                  .option("dbtable", self.args.tables.adgroup)
                  .option("user", self.args.username)
                  .option("password", self.args.password)
                  .load()
                )
            self.adgroup_df = self.adgroup_df.withColumn("Name", unbase64(col("Name")).cast("string"))
            self.adgroup_df = self.adgroup_df.withColumn("DelYN", col("DelYN").cast("int"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-ADGROUP-FROM-MSSQL]{self.args}")
            raise e
    
    def read_campaign_from_mssql(self):
        try:
            self.campaign_df = (spark
                  .read
                  .format("jdbc")
                  .option("url", self.args.mssql_url)
                  .option("dbtable", self.args.tables.campaign)
                  .option("user", self.args.username)
                  .option("password", self.args.password)
                  .load()
                )
            self.campaign_df = self.campaign_df.withColumn("Name", unbase64(col("Name")).cast("string"))
            self.campaign_df = self.campaign_df.withColumn("DelYN", col("DelYN").cast("int"))
#             self.campaign_df = self.campaign_df.withColumn("BaseUrl", unbase64(col("BaseUrl")).cast("string"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-CAMPAIGN-FROM-MSSQL]{self.args}")
            raise e
            
    def read_adgroupschedule_from_mssql(self):
        try:
            self.adgroupschedule_df = (spark
                  .read
                  .format("jdbc")
                  .option("url", self.args.mssql_url)
                  .option("dbtable", self.args.tables.adgroupschedule)
                  .option("user", self.args.username)
                  .option("password", self.args.password)
                  .load()
                )
            self.adgroupschedule_df = self.adgroupschedule_df.withColumn("DelYN", col("DelYN").cast("int"))
            self.adgroupschedule_df = self.adgroupschedule_df.withColumn("StartTime", date_format(col("StartTime"), "HH:mm:SS"))
            self.adgroupschedule_df = self.adgroupschedule_df.withColumn("EndTime", date_format(col("EndTime"), "HH:mm:SS"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-ADGROUPSCHEDULE-FROM-MSSQL]{self.args}")
            raise e
            
    def read_impressionhistory_from_mssql(self):
        try:
            self.impressionhistory_df = (spark
                  .read
                  .format("jdbc")
                  .option("url", self.args.mssql_url)
                  .option("dbtable", self.args.tables.impressionhistory)
                  .option("user", self.args.username)
                  .option("password", self.args.password)
                  .load()
                )
            self.impressionhistory_df = self.impressionhistory_df.withColumn("del_yn", col("del_yn").cast("int"))
#             self.impressionhistory_df = self.impressionhistory_df.withColumn("StartTime", date_format(col("StartTime"), "HH:mm:SS"))
#             self.impressionhistory_df = self.impressionhistory_df.withColumn("EndTime", date_format(col("EndTime"), "HH:mm:SS"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-IMPRESSIONHISTORY-FROM-MSSQL]{self.args}")
            raise e
            
    def read_audiencegroupinfo_from_mssql(self):
        try:
            self.audiencegroupinfo_df = (spark
                  .read
                  .format("jdbc")
                  .option("url", self.args.mssql_url)
                  .option("dbtable", self.args.tables.audiencegroupinfo)
                  .option("user", self.args.username)
                  .option("password", self.args.password)
                  .load()
                )
            self.audiencegroupinfo_df = self.audiencegroupinfo_df.withColumn("DelYN", col("DelYN").cast("int"))
#             self.impressionhistory_df = self.impressionhistory_df.withColumn("StartTime", date_format(col("StartTime"), "HH:mm:SS"))
#             self.impressionhistory_df = self.impressionhistory_df.withColumn("EndTime", date_format(col("EndTime"), "HH:mm:SS"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-AUDIENCEGROUPINFO-FROM-MSSQL]{self.args}")
            raise e
            
    def read_advertiserinfo_from_mssql(self):
        try:
            self.advertiserinfo_df = (spark
              .read
              .format("jdbc")
              .option("url", self.args.mssql_url)
              .option("dbtable", self.args.tables.advertiserinfo)
              .option("user", self.args.username)
              .option("password", self.args.password)
              .load()
            )
            self.advertiserinfo_df = self.advertiserinfo_df.withColumn("UseYN", col("UseYN").cast("int"))
        except Exception as e:
            mssql_log.error(f"[FAIL-READ-ADVERTISERINFO-FROM-MSSQL]{self.args}")
            raise e

    def write_inventory_to_delta(self):
        try:
            self.inventory_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_inventory")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-INVENTORY-TO-DELTA]{self.args}")
            raise e
    
    def write_adgroup_to_delta(self):
        try:
            self.adgroup_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_adgroup")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-ADGROUP-TO-DELTA]{self.args}")
            raise e
    
    def write_campaign_to_delta(self):
        try:
            self.campaign_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_campaign")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-CAMPAIGN-TO-DELTA]{self.args}")
            raise e
    
    def write_adgroupschedule_to_delta(self):
        try:
            self.adgroupschedule_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_adgroupschedule")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-ADGROUPSCHEDULE-TO-DELTA]{self.args}")
            raise e
    
    def write_impressionhistory_to_delta(self):
        try:
            self.impressionhistory_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_impressionhistory")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-IMPRESSIONHISTORY-TO-DELTA]{self.args}")
            raise e
            
    def write_audiencegroupinfo_to_delta(self):
        try:
            self.audiencegroupinfo_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_audiencegroupinfo")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-AUDIENCEGROUPINFO-TO-DELTA]{self.args}")
            raise e
            
    def write_advertiserinfo_to_delta(self):
        try:
            self.advertiserinfo_df.write.format("delta").mode("append").options(header="true").saveAsTable("ice.cm_advertiserinfo")
        except Exception as e:
            mssql_log.error(f"[FAIL-WRITE-ADVERTISERINFO-TO-DELTA]{self.args}")
            raise e
    
    def delete_prev_inventory(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_inventory").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_inventory")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-INVENTORY]{self.args}")
            raise e

    def delete_prev_adgroup(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_adgroup").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_adgroup")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-ADGROUP]{self.args}")
            raise e
    
    def delete_prev_campaign(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_campaign").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_campaign")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-CAMPAIGN]{self.args}")
            raise e
            
    def delete_prev_adgroupschedule(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_adgroupschedule").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_adgroupschedule")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-ADGROUPSCHEDULE]{self.args}")
            raise e
            
    def delete_prev_impressionhistory(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_impressionhistory").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_impressionhistory")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-IMPRESSIONHISTORY]{self.args}")
            raise e
            
    def delete_prev_audiencegroupinfo(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_audiencegroupinfo").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_audiencegroupinfo")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-AUDIENCEGROUPINFO]{self.args}")
            raise e
            
    def delete_prev_advertiserinfo(self):
        try:
            if spark.sql("show tables in ice").filter(col("tableName") == "cm_advertiserinfo").count() > 0:
                spark.sql(f"DELETE FROM ice.cm_advertiserinfo")
        except Exception as e:
            mssql_log.error(f"[FAIL-DELETE-PREV-ADVERTISERINFO]{self.args}")
            raise e
    
    def create_connection(self):
        # todo
        pass
    
    def call_health_check(self):
        # todo
        pass
    
    def is_valid_data(self):
        # todo
        pass

# COMMAND ----------

args = dict()
args = AttrDict(args)

# COMMAND ----------

args.config_file_name = "mssql_live_config_new_test.pickle"
args.domains = [
    "inventory", 
    "adgroup", 
    "campaign",
    "adgroupschedule",
    "advertiserinfo"
    
#     "impressionhistory",
#     "audiencegroupinfo"
]

args.tables = AttrDict({
    "inventory": "dbo.Inventory",
    "adgroup": "dbo.AdGroup",
    "campaign": "dbo.Campaign",
    "adgroupschedule": "dbo.AdGroupSchedule",
    "advertiserinfo": "dbo.AdvertiserInfo"
#     "impressionhistory": "dbo.impressionHistory",
#     "audiencegroupinfo" : "dbo.AudienceGroupInfo"
})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load MSSQL Config File

# COMMAND ----------

with open(f"/dbfs/FileStore/configs/{args.config_file_name}", "rb") as f:
    config = pickle.load(f)

# COMMAND ----------

# config

# COMMAND ----------

args.update(config)

# COMMAND ----------

# args.mssql_url

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

mssql_worker = MssqlWorker(args)

# COMMAND ----------

mssql_log.info(f"[CHECK-ARGS(READ-TABLES-FROM-MSSQL)]{mssql_worker.args}")

# COMMAND ----------

for domain in mssql_worker.args.domains:
    getattr(mssql_worker, f"read_{domain}_from_mssql")()
    getattr(mssql_worker, f"delete_prev_{domain}")()
    getattr(mssql_worker, f"write_{domain}_to_delta")()

# COMMAND ----------

# mssql_worker.advertiserinfo_df.display()

# COMMAND ----------

# mssql_worker.impressionhistory_df.display()

# COMMAND ----------

# mssql_worker.args.domains

# COMMAND ----------

# df = mssql_worker.adgroupschedule_df.select(date_format(col("EndTime"), "HH:mm:SS").alias("time"))

# COMMAND ----------

# df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delete Previous Data

# COMMAND ----------

# spark.sql("delete from ice.cm_adgroupschedule")

# COMMAND ----------

# display(dbutils.fs.ls(f"/mnt/ptbwa-basic"))

# COMMAND ----------

