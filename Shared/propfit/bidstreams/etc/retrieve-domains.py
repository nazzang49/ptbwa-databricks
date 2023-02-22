# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Retrieve Domains
# MAGIC - retrieval from bid-streams imp
# MAGIC - upload to s3 with unique domain list

# COMMAND ----------

!pip install attrdict

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import os

from attrdict import *
from datetime import date, timedelta, datetime

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
custom_log = log4jLogger.LogManager.getLogger("RETRIEVE-DOMAIN-LIST")

# COMMAND ----------

class DomainRetriever:
    def __init__(self, args) -> None:
        self.args = AttrDict(args)
        self.set_paths()
        self.set_query()
        self.flag = True
        
    def proc_all(self):
        self.read_from_delta() # d-1 imp
        self.upload_to_s3()
        
        # (!) if increment
        if self.flag:
            pass
#             self.read_from_s3() # d-2 latest
#             self.retrieve_unique_domains()
#             self.upload_to_s3()
        
    def set_query(self):
        self.args.read_query = f"""
            SELECT distinct BidStream.bundle_domain as domain, BidStream.bidrequest_type as requestType
            FROM ice.streams_imp_bronze_app_nhn
            WHERE cast(BidStream.reg_date AS DATE) = '{self.args.y1_date_str}' and BidStream.bidrequest_type = 4
        """
        
    def set_paths(self):
        # (!) ========================== path is changeable ==========================
        self.args.read_paths = AttrDict({
#             "latest": f"/mnt/ptbwa-basic/domains/latest/2022/05/30"
            "latest": f"/mnt/ptbwa-basic/domains/latest/{self.args.y2_date_year}/{self.args.y2_date_month}/{self.args.y2_date_day}"
        })
        
        self.args.write_paths = AttrDict({
#             "latest": f"/mnt/ptbwa-basic/domains/latest/2022/05/31"            
            "latest": f"/mnt/ptbwa-basic/domains/latest/{self.args.y1_date_year}/{self.args.y1_date_month}/{self.args.y1_date_day}",
        })
        
    def is_folder_exist(self):
        try:
            if not os.path.exists(f"/dbfs{self.args.read_paths.latest}"):
                self.t_df.write.mode("overwrite").parquet(self.args.write_paths.latest)
                self.flag = False
        except Exception as e:
            raise e
        
    def read_from_s3(self):
        try:
            self.l_df = spark.read.format("parquet").load(self.args.read_paths.latest)
        except Exception as e:
            custom_log.error("[FAIL-READ-FROM-S3]")
            raise e
    
    def read_from_delta(self):
        try:
            self.t_df = spark.sql(self.args.read_query)
        except Exception as e:
            custom_log.error("[FAIL-READ-FROM-DELTA]")
            raise e
    
    def retrieve_unique_domains(self):
        try:
            self.l_df = self.l_df.union(self.t_df).distinct()
        except Exception as e:
            custom_log.error("[FAIL-RETRIEVE-UNIQUE-DOMAINS]")
            raise e
    
    def upload_to_s3(self):
        try:
            self.t_df.write.mode("overwrite").parquet(self.args.write_paths.latest)
        except Exception as e:
            custom_log.error("[FAIL-UPLOAD-TO-S3]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing
# MAGIC - Get D-2 File
# MAGIC - Get D-1 Imps
# MAGIC - Union & Distinct
# MAGIC - Write D-1 File

# COMMAND ----------

args = AttrDict()
args.adxs = [
    "nhn",
]

t_date = datetime.now() + timedelta(hours=9)
args.y1_date = t_date - timedelta(days=1)
args.y2_date = t_date - timedelta(days=2)

args.y1_date_year, args.y1_date_month, args.y1_date_day = datetime.strftime(args.y1_date, "%Y-%m-%d").split("-")
args.y2_date_year, args.y2_date_month, args.y2_date_day = datetime.strftime(args.y2_date, "%Y-%m-%d").split("-")

args.y1_date_str = datetime.strftime(args.y1_date, "%Y-%m-%d")
args.y2_date_str = datetime.strftime(args.y2_date, "%Y-%m-%d")

# COMMAND ----------

t_date

# COMMAND ----------

domain_retriever = DomainRetriever(args)

# COMMAND ----------

import pprint

pprint.pprint(domain_retriever.args)

# COMMAND ----------

domain_retriever.proc_all()

# COMMAND ----------

domain_retriever.t_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

# df = spark.sql(q)

# COMMAND ----------

# df.select("*", "_metadata").display()

# COMMAND ----------

# tmp = domain_retriever.l_df.subtract(domain_retriever.t_df)

# COMMAND ----------

# df = spark.read.parquet("dbfs:/mnt/ptbwa-basic/domains/latest/2022/10/28/part-00000-tid-5144465426563439256-2c72f027-d626-460e-b8e5-3c64631fec7e-244328-1-c000.snappy.parquet")