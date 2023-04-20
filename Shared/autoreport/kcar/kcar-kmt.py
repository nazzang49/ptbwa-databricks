# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## KAKAO Moment API
# MAGIC - V2 :: Add Washing Logic `2022-11-30`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Logging

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("KAKAO-MOMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Packages

# COMMAND ----------

!python -m pip install AttrDict

# COMMAND ----------

import pickle
import os
import json
import pprint
import time
import pandas as pd
import sys
import requests

from urllib.parse import urlparse, parse_qs
from ptbwa_utils import *

from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from abc import ABC, abstractmethod
from typing import *
from collections import defaultdict

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### AutoReportUtils

# COMMAND ----------

class AutoReportUtils:
    """
    A class for dealing with repeated functions in autoreport
    """
    @staticmethod
    def parse_date(data_interval_end: str, minus_day: int) -> str:
        """
        A method for parsing data_interval_end to {simple date string}
        AS_IS: {2023-02-26 14:00:00}
        TO_BE: {2023-02-26}
        """
        try:
            data_interval_end = datetime.strptime(data_interval_end, '%Y-%m-%d %H:%M:%S') - timedelta(days=minus_day)
            return data_interval_end.strftime("%Y-%m-%d")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - env
# MAGIC   - dev
# MAGIC   - prod
# MAGIC - data_interval_end
# MAGIC   - `2023-02-27 11:40:05 (KST)`
# MAGIC   - Success or Fail

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### {env}

# COMMAND ----------

env = "prod"
# env = "dev"

# COMMAND ----------

try:
    # (!) LIVE
    env = dbutils.widgets.get("env")
    
#     (!) TEST
#     env = "dev"
except Exception as e:
    k_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

env

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### {data_interval_end}

# COMMAND ----------

data_interval_end = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")

# COMMAND ----------

try:
    # (!) LIVE
    data_interval_end = AutoReportUtils.parse_date(dbutils.widgets.get("data_interval_end"), 1) # D-1 = YESTERDAY
    
    # (!) TEST
#     data_interval_end = AutoReportUtils.parse_date("2023-03-02 14:00:00", 1)
except Exception as e:
    k_log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

data_interval_end

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### {database}

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

database

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Widget Params
# MAGIC - If Exist
# MAGIC   - Washing Process
# MAGIC   - Params from Airflow
# MAGIC     - Required
# MAGIC       - s_date = 시작일
# MAGIC       - e_date = 종료일
# MAGIC - If Not
# MAGIC   - Daily Process

# COMMAND ----------

# washing
s_date = None
e_date = None

# COMMAND ----------

# try:
#     # (!) PRODUCTION
#     s_date = dbutils.widgets.get("s_date")

#     # (!) TEST
# #     args["next_execution_date"] = "2022-11-28 16:28:38"
# except Exception as e:
#     k_log.info("[START-DATE-NOT-FOUND]THIS-IS-DAILY-PROCESS")

# COMMAND ----------

# try:
#     # (!) PRODUCTION
#     e_date = dbutils.widgets.get("e_date")

#     # (!) TEST
# #     args["next_execution_date"] = "2022-11-28 16:28:38"
# except Exception as e:
#     k_log.info("[END-DATE-NOT-FOUND]THIS-IS-DAILY-PROCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Class Definition
# MAGIC 1. API
# MAGIC 2. PROCESSOR

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### AutoReportAPI
# MAGIC - Parent for {channel}API classes
# MAGIC - Basic Setup of Attributes

# COMMAND ----------

class AutoReportAPI(ABC):
    """
    A class as parent class inherited by channels e.g. kakao-moment
    """
    
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def proc_all(self):
        pass
    
    @abstractmethod
    def _is_config_exist(self):
        pass
    
    @abstractmethod
    def _set_config(self):
        pass
    
    @abstractmethod
    def _set_client(self):
        pass
    
    @abstractmethod
    def is_valid(self):
        pass
    
    @abstractmethod
    def create_table_path(self):
        pass
    
    @abstractmethod
    def save_to_delta(self):
        pass
    
    @abstractmethod
    def delete_prev(self):
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Enum Control
# MAGIC - Arguments

# COMMAND ----------

class ArgumentGenerator(Enum):
    """
    A enum class for validating arguments and generating common types
    """
    DOMAINS = [
        "ad_accounts",
        "campaigns",
        "ad_groups",
        "creatives",
        "creatives_report",
    ]
    
class FieldGenerator(Enum):
    """
    A enum class for generating field parameter
    """
    pass
    
class ParamGenerator(Enum):
    """
    A enum class for generating param parameter
    """
    AD_ACCOUNTS = ""
    CAMPAIGNS = ""
    AD_GROUPS = ""
    CREATIVES = ""
    CREATIVES_REPORT = ""

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
            k_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save Client

# COMMAND ----------

# data = {
#     "client_id": "2cb2275ee440e296fadca45b9db08405",
#     "client_secret": "cg7yS53u4SKEdswF4XndudHdLpadRQHY",
#     "app_id": "771875",
#     'access_token': 'bacbRamVF2z2yt_VjS0BeZjTG6c45f6B0rLjLdWvCj1ylwAAAYM0hLpu',
#     'token_type': 'bearer',
#     'refresh_token': 'WCJqeriuoy6_1Qv9NrejVtwcogJv0-IQSKGu_lw7Cj1ylwAAAYM0hLps',
#     'expires_in': 21599,
#     'refresh_token_expires_in': 5183999
# }

# COMMAND ----------

# with open("/dbfs/FileStore/configs/kakao_moment_api_client.pickle", "wb") as f:
#     pickle.dump(data, f)

# COMMAND ----------

# with open("/dbfs/FileStore/configs/kakao_moment_api_client.pickle", "rb") as f:
#     config = pickle.load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### KAKAO Moment API

# COMMAND ----------

class KakaoMomentApi(AutoReportAPI):
    """
    A class for processing Kakao Moment API procedures
    args:
        customer_id: account_id in Kakao Moment
    """
    def __init__(self, customer_id, s_date=None, e_date=None) -> None:
        self.base_url = "https://apis.moment.kakao.com"
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        
        self.s_date = data_interval_end
        self.e_date = data_interval_end
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "kakao_moment_api_client.pickle"

        self.channel = "kmt"
        self.advertiser = "kcar"
        self.customer_id = customer_id
        
        self.base_db = database
        
        # (!) daily or washing
        self.is_washing = False
        if s_date and e_date:
            
            
            # (!) ================================================================== washing
#             self.is_washing = True


            self.s_date = s_date
            self.e_date = e_date
            
        self.table_path = self.create_table_path()
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()

        self.get_campaigns()
        self.get_adgroups()
        self.get_creatives()
        self.get_creatives_report()
        
        self.proc_join()
        self.rename_columns()
        
        self.delete_prev()
        self.save_to_delta()

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
            k_log.error(f"[FAIL-IS-CONFIG-EXIST]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting config as dict-based credentials
        """
        try:
            self._is_config_exist()
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            k_log.info(f"[CHECK-CONFIG]{self.config}")
        except Exception as e:
            k_log.error("[FAIL-SET-CONFIG]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for initiating kakao moment api by access token
        """
        try:
            self.client = self.config
            self._refresh_token()
            k_log.info(f"[CHECK-CONFIG-INFO]{self.config}")
        except Exception as e:
            k_log.error(f"[FAIL-SET-CLIENT]{self.config}")
            raise e
            
    def _refresh_token(self):
        """
        A method for refreshing token to disable expiration of access token
        """
        try:
            url = "https://kauth.kakao.com/oauth/token"
            data = {
                "grant_type": "refresh_token",
                "client_id": self.client["client_id"],
                "client_secret": self.client["client_secret"],
                "refresh_token": self.client['refresh_token']
            }

            response = requests.post(url, data=data)
            result = response.json()
            PtbwaUtils.check_result("REFRESH-TOKEN", result)

            if 'access_token' in result:
                self.client['access_token'] = result['access_token']

            if 'refresh_token' in result:
                self.client['refresh_token'] = result['refresh_token']
            else:
                print("REFRESH-TOKEN:NOT-EXPIRED")
                pass

            # (!) update config file
            with open(self.config_path, "wb") as f:
                pickle.dump(self.client, f)

        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def rename_columns(self):
        """
        A method for renaming columns of spark df
        """
        try:
            for col in self.spark_creatives_report_df.schema:
                renamed_col = col.name.replace("metrics.", "metrics_")
                renamed_col = self.convert_snake_to_camel(renamed_col)
                self.spark_creatives_report_df = self.spark_creatives_report_df.withColumnRenamed(col.name, renamed_col)
            k_log.info(f"[SUCCESS-{sys._getframe().f_code.co_name.upper()}-SCHEMA]{self.spark_creatives_report_df.schema}")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def convert_from_pandas_to_spark(self, domain):
        """
        A method for converting from pandas to spark
        """
        try:
            setattr(self, f"spark_{domain}_df", spark.createDataFrame(getattr(self, f"{domain}_df")))
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
    
    def convert_to_pandas(self, domain, result):
        """
        A method for converting to pandas df after getting result from each API
        """
        try:
            setattr(self, f"{domain}_df", pd.json_normalize(data=result))
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-DOMAIN]{domain}")
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-RESULT]{domain}")
    
    def get_campaigns(self):
        """
        A mehtod for getting campaigns e.g. id
        read-level: adAccount
        """
        try:
            domain = "CAMPAIGNS"
            params = self.get_params_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_headers = self.create_request_headers(domain.lower())
            request_params = self.create_request_params(domain.lower(), params)
            response = requests.get(url=request_url, params=request_params, headers=request_headers)
            
            result = response.json()
            PtbwaUtils.check_result(domain, result)
            
            self.convert_to_pandas(domain.lower(), result["content"])
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_adgroups(self):
        """
        A mehtod for getting adgroups e.g. id
        read-level: campaign
        """
        try:
            assert self.campaigns_df is not None, f"[CAMPAIGNS-DATA-REQUIRED]CALL-GET-CAMPAIGNS-FIRST"
            domain = "AD_GROUPS"
            params = self.get_params_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_headers = self.create_request_headers(domain.lower())
            request_params = self.create_request_params(domain.lower(), params)
            
            results = []
            for campaign_id, campaign_name in zip(self.campaigns_df["id"].tolist(), self.campaigns_df["name"].tolist()):
                request_params["campaignId"] = campaign_id
                
                # (!) need "on/off" on previous data
#                 request_params["config"] = ["ON"]
                response = requests.get(url=request_url, params=request_params, headers=request_headers)
                result = response.json()
        
                # (!) add campaign
                for adgroup in result["content"]:
                    adgroup["campaignId"] = campaign_id
                    adgroup["campaignName"] = campaign_name
                
                results += result["content"]
                time.sleep(5.5)
            
            PtbwaUtils.check_result(domain, results)
            self.convert_to_pandas(domain.lower(), results)
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_creatives(self):
        """
        A mehtod for getting creatives e.g. id
        read-level: adGroup
        """
        try:
            assert self.ad_groups_df is not None, f"[ADGROUPS-DATA-REQUIRED]CALL-GET-ADGROUPS-FIRST"
            domain = "CREATIVES"
            params = self.get_params_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_headers = self.create_request_headers(domain.lower())
            request_params = self.create_request_params(domain.lower(), params)
            
            results = []
            for adgroup_id, adgroup_name, campaign_id, campaign_name in zip(self.ad_groups_df["id"].tolist(), 
                                                                            self.ad_groups_df["name"].tolist(), 
                                                                            self.ad_groups_df["campaignId"].tolist(), 
                                                                            self.ad_groups_df["campaignName"].tolist()):
                request_params["adGroupId"] = adgroup_id
                
                # (!) need "on/off" on previous data
#                 request_params["config"] = ["ON"]
                response = requests.get(url=request_url, params=request_params, headers=request_headers)
#                 time.sleep(5.5)
                result = response.json()
    
                # (!) add campaign
                for creative in result["content"]:
                    creative["adgroupId"] = adgroup_id
                    creative["adgroupName"] = adgroup_name
                    creative["campaignId"] = campaign_id
                    creative["campaignName"] = campaign_name
    
                results += result["content"]
            
            PtbwaUtils.check_result(domain, results)
            self.convert_to_pandas(domain.lower(), results)
            self.convert_from_pandas_to_spark(domain.lower())
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_creatives_report(self):
        """
        A mehtod for getting creatives report e.g. metrics
        read-level: creatives
        """
        try:
            assert self.creatives_df is not None, f"[CREATIVES-DATA-REQUIRED]CALL-GET-CREATIVES-FIRST"
            domain = "CREATIVES_REPORT"
            params = self.get_params_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_headers = self.create_request_headers(domain.lower())
            request_params = self.create_request_params(domain.lower(), params)
            
            n_groups = (len(self.creatives_df) // 100) + 1 # (!) max 100 per 1-request
            PtbwaUtils.check_result("CREATIVES-GROUPS", n_groups)
            ids = self.creatives_df["id"].tolist()

            results = []
            for i in range(n_groups):
                request_params["creativeId"] = ids[100 * i:100 * (i + 1)]
                
                # (!) when you need previous data
                request_params["start"] = self.s_date.replace("-", "")
                request_params["end"] = self.e_date.replace("-", "")
                
#                 request_params["start"] = "20221128"
#                 request_params["end"] = "20221201"
                
                # (!) when you need yesterday data (scheduling job)
#                 request_params["datePreset"] = "YESTERDAY"
                request_params["metricsGroup"] = "BASIC,ADDITION,MESSAGE,MESSAGE_ADDITION"
                response = requests.get(url=request_url, params=request_params, headers=request_headers)
                result = response.json()
            
                if "data" in result:
                    results += result["data"]
                    
                PtbwaUtils.check_result(domain, result)
                time.sleep(5.5)
            
            self.convert_to_pandas(domain.lower(), results)
            self.convert_from_pandas_to_spark(domain.lower())
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def proc_join(self):
        """
        A method for processing join on creatives - creatives_report
        """
        try:
            self.spark_creatives_report_df = self.spark_creatives_report_df.withColumnRenamed("dimensions.creative_id", "id")
            self.spark_creatives_report_df = self.spark_creatives_report_df.join(self.spark_creatives_df, on=["id"], how="inner")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.spark_creatives_report_df.show(10)}")
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.spark_creatives_df.show(10)}")
            raise e
    
    def create_request_params(self, domain, params=None):
        try:
            request_params = dict()
            if params:
                for param in params.split("&"):
                    tmp = param.split("=")
                    request_params[tmp[0]] = tmp[1]
            
            PtbwaUtils.check_result("REQUEST-PARAMS", request_params)
            return request_params
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-FIELDS]{fields}")
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-PARAMS]{params}")
            raise e
    
    def create_request_headers(self, domain):
        """
        A method for creating api request headers
        default:
            Authorization
            adAccountId
        """
        try:
            request_headers = {
                "Authorization": f"Bearer {self.client['access_token']}",
                "adAccountId": f"{self.customer_id}"
            }
            
            PtbwaUtils.check_result("REQUEST-HEADERS", request_headers)
            return request_headers
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
            
    def create_request_url(self, domain, id=None):
        """
        A method for creating api request url
        """
        try:
            assert domain in ArgumentGenerator.DOMAINS.value, f"[NOT-FOUND-DOMAINS]{domain}"
            
            if "report" in domain:
                tmp = domain.split("_")
                request_url = f"{self.base_url}/openapi/v4/{tmp[0]}/{tmp[1]}"
            else:
                request_url = f"{self.base_url}/openapi/v4/{self.convert_snake_to_camel(domain)}"
            PtbwaUtils.check_result("REQUEST-URL", request_url)
            return request_url
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-DOMAIN]{domain}")
            raise e
    
    def convert_snake_to_camel(self, value):
        """
        A method for converting snake to camel
        """
        return ''.join(w.title() if i != 0 else w for i, w in enumerate(value.split('_')))
    
    def is_valid_dict(self, d):
        """
        A method for validating final json rows e.g. columns
        """
        try:
            cols =[
                "videoPlayActions"
            ]

            for col in cols:
                if not d.get(col):
                    d[col] = "0"
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{d.keys()}")
            raise e
            
    def get_params_str(self, domain):
        """
        A method for creating params string as request data
        """
        try:
            return getattr(ParamGenerator, domain).value
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
        common_path = f"{self.base_db}.{self.channel}_{self.advertiser}"
        return f"{common_path}_ad_wash_stats" if self.is_washing else f"{common_path}_ad_stats"
    
    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
            (self.spark_creatives_report_df
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path))
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous data
        """
        try:
            # (!) check table exist
#             if (spark.sql("show tables in <DATABASE>").filter(col("tableName") == "<TABLENAME>").count() > 0):
#                 pass
            spark.sql(f"delete from {self.table_path} where start between '{self.s_date}' and '{self.e_date}'")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Manual Processing

# COMMAND ----------

# s_date = "2023-01-13"
# e_date = "2023-01-15"

# COMMAND ----------

customer_id = "153963"
kakao_moment_api = KakaoMomentApi(customer_id, s_date, e_date)

# COMMAND ----------

try:
    kakao_moment_api.proc_all()
except Exception as e:
    if "USING column `id` cannot be resolved on the left side of the join." in str(e):
        k_log.info(f"[NODATA-KCAR-KMT]DATE::{kakao_moment_api.s_date}")
    else:
        k_log.error("[FAIL-KCAR-KMT]")
        raise e

# COMMAND ----------

kakao_moment_api.creatives_report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Wash Table

# COMMAND ----------

# df = spark.createDataFrame([], spark.sql("select * from auto_report.kmt_kcar_ad_stats").schema)
# df.write.format("delta").mode("append").saveAsTable("auto_report.kmt_kcar_ad_wash_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Simple Agg

# COMMAND ----------

# from pyspark.sql.functions import *

# kakao_moment_api.spark_creatives_report_df.agg(sum("metricsImp")).show()

# COMMAND ----------

# kakao_moment_api.campaigns_df
# kakao_moment_api.ad_groups_df
# kakao_moment_api.creatives_df
# kakao_moment_api.creatives_report_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Spark DF

# COMMAND ----------

# kakao_moment_api.spark_creatives_report_df = kakao_moment_api.spark_creatives_report_df.filter(expr("id is not null"))

# COMMAND ----------

# kakao_moment_api.spark_creatives_report_df.join(kakao_moment_api.spark_creatives_df, on=["id"], how="inner").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save to Delta Manually

# COMMAND ----------

# kakao_moment_api.spark_creatives_report_df.display()

# COMMAND ----------

# table_name = "auto_report.kmt_kcar_ad_stats"
# kakao_moment_api.spark_creatives_report_df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Authentication
# MAGIC - get access token
# MAGIC - refresh token

# COMMAND ----------

# import requests
# import json

# url = "https://kauth.kakao.com/oauth/token"

# data = {
#     "grant_type" : "authorization_code",
#     "client_id" : "2cb2275ee440e296fadca45b9db08405",
#     "client_secret": "cg7yS53u4SKEdswF4XndudHdLpadRQHY",
#     "redirect_uri" : "http://localhost",
#     "code" : "9R0tB3hJPMCxbWl3sxgurOUChvr4yJeOqtiJuzETugGYUvOL7LZJVjHsSuKP_JCj8Boo_wopb1QAAAGDNHw0Hg"
# }

# response = requests.post(url, data=data)
# tokens = response.json()
# tokens

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### API Test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Accounts

# COMMAND ----------

# import requests
# import json

# base_url = "https://apis.moment.kakao.com"
# account_id = "153963"
# path = f"/openapi/v4/adAccounts/{account_id}"

# headers = {
#     "Authorization": "Bearer bacbRamVF2z2yt_VjS0BeZjTG6c45f6B0rLjLdWvCj1ylwAAAYM0hLpu",
#     "adAccountId": f"{account_id}"
# }

# response = requests.get(
#     base_url + path, 
#     headers=headers
# )
# result = response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Campaigns

# COMMAND ----------

# import requests
# import json

# base_url = "https://apis.moment.kakao.com"
# account_id = "153963"
# path = f"/openapi/v4/campaigns"

# params = {
#     "config": "ON"
# }

# headers = {
#     "Authorization": "Bearer bacbRamVF2z2yt_VjS0BeZjTG6c45f6B0rLjLdWvCj1ylwAAAYM0hLpu",
#     "adAccountId": f"{account_id}"
# }

# response = requests.get(
#     base_url + path, 
#     params=params, 
#     headers=headers
# )
# result = response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Creatives Report

# COMMAND ----------

# import requests
# import json

# base_url = "https://apis.moment.kakao.com"
# account_id = "153963"
# path = f"/openapi/v4/creatives/report"

# params = {
#     "start": "20220601",
#     "end": "20220630",
#     "creativeId": [17903785]
# }

# headers = {
#     "Authorization": "Bearer FI6hxJhzVBrspXchCM4hBS6Z1Htoc0jW9tGhTRzYCj11GwAAAYM5ulLT",
#     "adAccountId": f"{account_id}"
# }

# response = requests.get(
#     base_url + path, 
#     params=params, 
#     headers=headers
# )
# result = response.json()