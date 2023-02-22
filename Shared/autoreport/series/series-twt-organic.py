# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Series Twitter :: Organic Tweet
# MAGIC - Load & Mapping Organic Tweet to Delta Tables
# MAGIC - Mapping Info & Raw Data
# MAGIC   - https://docs.google.com/spreadsheets/d/1-MlTbtyMJVdHb55cSZwn3k-EgF2hM204Swl46bPPNSc/edit?usp=sharing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
t_log = log4jLogger.LogManager.getLogger("TWITTER-ADS(ORGANIC)")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install gspread
!python -m pip install gspread_dataframe
!python -m pip install AttrDict
!python -m pip install oauth2client

# COMMAND ----------

import pickle
import os
import json
import sys
import pprint
import gspread
import gspread_dataframe as gd

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from oauth2client.service_account import ServiceAccountCredentials

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - env
# MAGIC   - dev
# MAGIC   - prod

# COMMAND ----------

env = "prod"
# env = "dev"

# COMMAND ----------

try:
    # (!) LIVE
    env = dbutils.widgets.get("env")
    
    # (!) TEST
#     env = "dev"
except Exception as e:
    t_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

database

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Class

# COMMAND ----------

class PtbwaUtils:
    """
    A class for dealing with repeated functions
    """
    @staticmethod
    def check_result(domain, result):
        try:
            print(f"[{domain}-RESULT]")
            pprint.pprint(result)
            print()
            log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

class TwitterAdsApi:
    
    def __init__(self) -> None:
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        
        self.base_path = "/dbfs/FileStore/scheduler"
        self.config_name = "melodic_agency_343808_0acf75646ae4.json"
        self.url = "https://docs.google.com/spreadsheets/d/1-MlTbtyMJVdHb55cSZwn3k-EgF2hM204Swl46bPPNSc/edit?usp=sharing"
        self.sheet_names = ["Twitter_Organic Tweet", "Twitter_Organic_RAW"]
        
        self.json_rows = []
        self.channel = "twt"
        self.base_db = database
        self.advertiser = "series"
        self.table_path = self.create_table_path()
        
        # todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        # (!) common logic
        self._set_config()
        self.get_mapping_info()
        self.create_data_dict()
        
        # (!) get mapping info from gsheet
        self.create_mapping_info_df()

        # (!) get data from gsheet
        self.get_schema()
        self.create_organic_tweets_df()
        self.create_organic_tweets_report_df()

        self.delete_prev()
        self.save_to_delta()
        
    def sample_method(self):
        """
        A sample method
        """
        try:
            pass
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def is_valid(self):
        pass
    
    def _is_config_exist(self):
        """
        A method for checking config file
        """
        try:
            self.config_path = os.path.join(self.base_path, self.config_name)
            if not os.path.exists(self.config_path):
                raise ValueError("[NOT-FOUND-CONFIG]")
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting gsheet config as dict-based credentials
        """
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            self._is_config_exist()
            self.client = ServiceAccountCredentials.from_json_keyfile_name(self.config_path, scope)
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_mapping_info(self):
        """
        A method for getting mapping info sheets
        """
        try:
            self.sheet = (gspread
                          .authorize(self.client)
                          .open_by_url(self.url))
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def convert_sheet_to_dict(self, sheet_name):
        """
        A method for converting sheet to df by open source lib :: https://github.com/robin900/gspread-dataframe
        """
        try:
            df = (gd.get_as_dataframe(self.sheet.worksheet(sheet_name), 
                                   header=0, 
                                   skiprow=0, 
                                   usecol=[0, 1, 2, 3])
                .dropna(axis=1, how='all')  # drop null-only column
                .dropna(axis=0, how='all')) # drop null-only row
            return df
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
        
    def create_data_dict(self):
        """
        A method for creating mapping info df after get_mapping_info()
        """
        try:
            self.data_dict = {sheet_name: self.convert_sheet_to_dict(sheet_name) for sheet_name in self.sheet_names}
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
        
    def create_mapping_info_df(self):
        """
        A method for creating mapping info dict to spark df
        """
        try:
            self.mapping_info_df = (spark
                                    .createDataFrame(self.data_dict["Twitter_Organic Tweet"])
                                    .select(["최초 라이브", "Campaign name", "Creative name", "Tweet ID"])
                                    .withColumnRenamed("최초 라이브", "createdAt")
                                    .withColumnRenamed("Campaign Name", "campaignName")
                                    .withColumnRenamed("Creative name", "creativeName")
                                    .withColumnRenamed("Tweet ID", "tweetId")
                                    .withColumn("tweetId", col("tweetId").cast("long"))) # (!) cast as proper id-value e.g. 1.40000E => 140000
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_organic_tweets_df(self):
        """
        A method for creating mapping info dict to spark df
        """
        try:
#             ["캠페인 ID", "캠페인 이름", "광고 그룹 ID", "광고 그룹 이름", "트윗 ID", "광고 이름", "노출수", "앱 클릭수", "설치 횟수", "링크 클릭수", "비용", "트윗 참여"]
            self.organic_tweets_df = (spark
                                    .createDataFrame(self.data_dict["Twitter_Organic_RAW"])
                                    .withColumnRenamed("기간", "segmentDate")
                                    .withColumnRenamed("캠페인 이름", "campaignName")
                                    .withColumnRenamed("광고 그룹 이름", "lineItemName")
                                    .withColumnRenamed("트윗 ID", "tweetId")
                                    .withColumnRenamed("광고 이름", "tweetName")
                                    .withColumnRenamed("링크 클릭수", "metricsLinkClicks")
                                    .withColumnRenamed("설치 횟수", "metricsAppInstalls")
                                    .withColumnRenamed("비용", "metricsBilledChargeLocalMicro")
                                    .withColumnRenamed("트윗 참여", "metricsEngagements")
                                    .withColumnRenamed("노출수", "metricsImpressions")
                                    .withColumnRenamed("동영상 25% 재생", "metricsVideoViews25")
                                    .withColumnRenamed("동영상 50% 재생", "metricsVideoViews50")
                                    .withColumnRenamed("동영상 75% 재생", "metricsVideoViews75")
                                    .withColumnRenamed("동영상 재생 완료", "metricsVideoViews100")
                                    .withColumnRenamed("동영상 조회", "metricsVideoTotalView")
                                    .withColumnRenamed("앱 클릭수", "metricsAppClicks")
                                    .withColumn("tweetId", col("tweetId").cast("long")))
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_organic_tweets_report_df(self):
        """
        A method for creating organic tweets report df
        """
        try:
            self.organic_tweets_report_df = self.organic_tweets_df.drop("campaignName").join(self.mapping_info_df, on=["tweetId"], how="inner")
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_schema(self):
        """
        A method for getting schema from ad table
        """
        try:
            self.schema = spark.sql("select * from auto_report.twt_series_ad_stats limit 1").schema
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
        return f"{self.base_db}.{self.channel}_{self.advertiser}_organic_stats"
    
    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
            (self.organic_tweets_report_df
                 .write
                 .mode("overwrite")
                 .saveAsTable(self.table_path))
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous google ads
        """
        try:
            spark.sql(f"delete from {self.table_path} where segmentDate = '{self.yesterday}'")
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

twitter_ads_api = TwitterAdsApi()

# COMMAND ----------

twitter_ads_api.proc_all()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

twitter_ads_api.mapping_info_df.display()

# COMMAND ----------

twitter_ads_api.organic_tweets_df.display()

# COMMAND ----------

twitter_ads_api.organic_tweets_report_df.display()

# COMMAND ----------

# twitter_ads_api.organic_tweets_report_df.write.format("delta").mode("append").saveAsTable("auto_report.tt_twt")