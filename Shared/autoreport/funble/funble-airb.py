# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Airbridge MMP API

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Logging

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("AIRBRIDGE-MMP")

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
    k_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

database

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
    A class as parent class inherited by channels e.g. airbridge-mmp
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
# MAGIC ### Save Client Example

# COMMAND ----------

# data = {
#     'api_token': 'ab005bf7c8c546eca0497dc081de84ad',
#     'tracking_link_api_token': '81485e8b8be34264bb553d6106a4446d',
#     'task_id': '1-632d2045-4450467912edcc9951dbc6ae'
# }

# COMMAND ----------

# with open("/dbfs/FileStore/configs/airbridge_api_client.pickle", "wb") as f:
#     pickle.dump(data, f)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Airbridge MMP API

# COMMAND ----------

class AirbridgeApi(AutoReportAPI):
    """
    A class for processing Airbridge API procedures
    """
    def __init__(self) -> None:
        self.stat_base_url = "https://api.airbridge.io/reports/api/v6/apps"
        self.raw_base_url = "https://api.airbridge.io/log-export/api/v3/apps"
        self.prop_df = pd.read_csv("/dbfs/FileStore/configs/airbridge_properties.csv", delimiter=",", encoding="utf-8")
        
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "airbridge_api_client.pickle"

        self.channel = "airb"
        self.advertiser = "funble"
        self.base_db = database
        
#         todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        
        # (!) stats
        self.get_task_id()
        time.sleep(20)
        self.get_reports()
        
        # (!) raw
        self.create_folder()
        self.get_report_id()
        time.sleep(40)
        self.get_download_url()
        self.post_process()

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
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting config as dict-based credentials
        """
        try:
            self._is_config_exist()
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for initiating kakao moment api by access token
        """
        try:
            self.client = self.config
            k_log.info(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.client}")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.client}")
            raise e
            
    def create_folder(self):
        """
        A mehtod for creating folder to save raw data
        """
        try:
            if not os.path.exists(f"/dbfs/FileStore/autoreport/funble/{self.yesterday}"):
                dbutils.fs.mkdirs(f"/FileStore/autoreport/funble/{self.yesterday}")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_report_id(self):
        """
        A mehtod for getting {report id}
        """
        try:
            domain = "LOG_EXPORT"
            request_url = self.create_request_url(domain.lower(), "POST")
            request_headers = self.create_request_headers("POST")
            
            request_params = self.create_request_params(domain.lower())
            response = requests.post(request_url, headers=request_headers, json=request_params)
            result = response.json()
            self.report_id = result["data"]["reportID"]
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_download_url(self):
        """
        A mehtod for getting {report download url}
        """
        try:
            domain = "LOG_EXPORT"
            request_url = self.create_request_url(domain.lower(), "GET")
            request_headers = self.create_request_headers("GET")
            response = requests.get(request_url, headers=request_headers)
            
            self.download_url = response.json()["data"]["url"]
            self.create_table_path(domain.lower())
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def post_process(self):
        """
        A mehtod for doing post process by {report download url}
        """
        try:
            domain = "LOG_EXPORT"
            
            # (!) download csv
#             !wget -P /dbfs/FileStore/autoreport/funble/self.yesterday -O funble_raw.csv self.download_url
            response = requests.get(self.download_url)
            with open(f"/dbfs/FileStore/autoreport/funble/{self.yesterday}/funble_raw.csv", 'w') as f:
                f.writelines(response.content.decode("utf-8"))
    
            schema = spark.sql(f"select * from {self.table_path} limit 1").schema
            k_log.info(f"[SUCCESS-{sys._getframe().f_code.co_name.upper()}]{schema}")
            
            self.raw_df = (spark
                             .read
                             .format("csv")
                             .option("header", "true")
                             .schema(schema)
                             .load(f"dbfs:/FileStore/autoreport/funble/{self.yesterday}/funble_raw.csv"))
            
            for col in self.raw_df.schema:
                renamed_col = col.name.replace(" ", "")
                self.raw_df = self.raw_df.withColumnRenamed(col.name, renamed_col)
            
            self.delete_prev(domain.lower())
            self.save_to_delta(domain.lower())
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_task_id(self):
        """
        A mehtod for getting {task id}
        """
        try:
            domain = "ACTUAL_QUERY"
            request_url = self.create_request_url(domain.lower(), "POST")
            request_headers = self.create_request_headers("POST")
            
            request_params = self.create_request_params(domain.lower())
            response = requests.post(request_url, headers=request_headers, json=request_params)
            result = response.json()
            self.task_id = result["data"]["taskId"]
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_reports(self):
        """
        A method for getting reports asynchronously
        """
        try:
            domain = "ACTUAL_QUERY"
            request_url = self.create_request_url(domain.lower(), "GET")
            request_headers = self.create_request_headers("GET")
            
            # (!) turn on if you need pagination
#             default_params = {
#                 "skip": "0",
#                 "size": "10"
#             }
            response = requests.get(request_url, headers=request_headers)
            self.tmp = response.json()
            self.results = []
            for row in response.json()["data"]["stats"]:
                self.results.append(json.dumps(row))
                
            self.create_table_path(domain.lower())
            self.create_json_rdd()
            self.rename_columns()
            
            self.delete_prev(domain.lower())
            self.save_to_delta(domain.lower())
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def rename_columns(self):
        """
        A method for renaming columns of spark DF
        """
        try:
            for col in self.df.schema:
                renamed_col = self.convert_snake_to_camel(col.name)
                self.df = self.df.withColumnRenamed(col.name, renamed_col)
            k_log.info(f"[SUCCESS-{sys._getframe().f_code.co_name.upper()}-SCHEMA]{self.df.schema}")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_json_rdd(self):
        """
        A method for creating spark DF before saving to delta
        """
        try:
            json_rdd = sc.parallelize(self.results)
            self.df = spark.read.json(json_rdd)
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.results[0]}")
            raise e
    
    def create_request_params(self, domain):
        """
        A method for creating request params to get {task id}
        """
        try:
            request_params = {
                 "dimensions": [
                      "event_date",
                      "channel",
                 ],
                 "metrics": [
                      "impressions",
                      "clicks",
                      "impressions_channel",
                      "clicks_channel",
                      "cost_channel",
                      "app_events",
                      "app_event_value",
                      "app_attributed_events",
                      "app_installs",
                      "app_first_installs",
                      "app_re_installs",
                      "app_attributed_installs",
                      "app_unattributed_installs",
                      "app_deferred_deeplink_installs",
                      "app_uninstalls"
                 ],
                # (!) turn on if you need
#                  "filters": [
#                       {
#                            "values": [
#                                 "2022-09"
#                            ],
#                            "dimension": "event_date",
#                            "filterType": "LIKE"
#                       }
#                  ],
                 "sorts": [
                      {
                           "isAscending": True,
                           "fieldName": "event_date"
                      }
                 ],
                 "from": f"{self.yesterday}", # (!) max 100 rows per 1-request
                 "to": f"{self.yesterday}"
            }
            
            if "log" in domain:
                request_params = {
                    # (!) yesterday criteria
                    "dateRange": {
                        "start": f"{self.yesterday}",
                        "end": f"{self.yesterday}"
#                         "start": f"2022-09-01",
#                         "end": f"2022-09-25"
                    },
                    "events": [
                        "app_install",
                        "app_open",
                        "app_deeplink_open",
                        "app_sign_up",
                        "app_sign_in",
                        "app_sign_out",
                        "app_home_screen",
                        "app_product_catalog",
                        "app_search_results",
                        "app_product_view",
                        "app_add_to_cart",
                        "app_order_complete",
                        "app_order_cancel",
                        "app_ad_impression",
                        "app_uninstall"
                    ],
                    "properties": self.prop_df["Property Key"].tolist()
                }
            
            PtbwaUtils.check_result("REQUEST-PARAMS", request_params)
            return request_params
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_request_headers(self, method):
        """
        A method for creating api request headers
        default:
            Authorization
            accept
            content-type
        """
        try:
            request_headers = {
                "Authorization": f"AIRBRIDGE-API-KEY {self.client['api_token']}",
                "accept": "application/json",
            }
            
            if method == "POST":
                request_headers["content-type"] = "application/json"
            
            PtbwaUtils.check_result("REQUEST-HEADERS", request_headers)
            return request_headers
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def create_request_url(self, domain, method):
        """
        A method for creating api request url
        """
        try:
            tmp = domain.split("_")
            request_url = f"{self.stat_base_url}/{self.advertiser}/{tmp[0]}/{tmp[1]}"
            if method == "GET":
                request_url = f"{request_url}/{self.task_id}"
                
            if "log" in domain:
                request_url = f"{self.raw_base_url}/{self.advertiser}"
                if method == "GET":
                    request_url = f"{request_url}/request/{self.report_id}"
                else:
                    request_url = f"{request_url}/app/request"
                
            PtbwaUtils.check_result("REQUEST-URL", request_url)
            return request_url
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def convert_snake_to_camel(self, value):
        """
        A method for converting snake to camel
        """
        return ''.join(w.title() if i != 0 else w for i, w in enumerate(value.split('_')))
    
    def create_table_path(self, domain):
        """
        A method for creating saving point path e.g. stats, raw
        """
        base_path = f"{self.base_db}.{self.channel}_{self.advertiser}"
        self.table_path = f"{base_path}_mmp_raw" if "log" in domain else f"{base_path}_mmp_stats"
    
    def save_to_delta(self, domain):
        """
        A method for saving stat data to delta
        """
        try:
            if "log" in domain:
                (self.raw_df
                     .write
                     .mode("append")
                     .saveAsTable(self.table_path)
                )
            else:
                (self.df
                     .fillna(0)
                     .write
                     .mode("append")
                     .saveAsTable(self.table_path)
                )
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e
            
    def delete_prev(self, domain):
        """
        A method for deleting previous data e.g. yesterday
        """
        try:
            if "log" in domain:
                spark.sql(f"delete from {self.table_path} where EventDatetime >= '{self.yesterday}' and EventDatetime < '{self.today}'")
            else:
                spark.sql(f"delete from {self.table_path} where eventDate = '{self.yesterday}'")
        except Exception as e:
            k_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

# requests.get(airbridge_api.download_url)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# FUNBLE
airbridge_api = AirbridgeApi()

# COMMAND ----------

try:
    airbridge_api.proc_all()
except Exception as e:
    k_log.error("[FAIL-FUNBLE-AIRB]")
    raise e

# COMMAND ----------

# airbridge_api.post_process()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Properties
# MAGIC - https://docs.google.com/spreadsheets/d/13ThNnryqRmZQwJVecM8lZ2OB7LzA8s3cwLlQBGpbXLg/edit#gid=723627948

# COMMAND ----------

# df = pd.read_csv("/dbfs/FileStore/configs/airbridge_properties.csv", delimiter=",", encoding="utf-8")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save to Delta Manually

# COMMAND ----------

# table_name = "auto_report.airb_funble_mmp_raw"
# df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Define Schema

# COMMAND ----------

# cols = []
# for row in airbridge_api.prop_df.iterrows():
#     r = row[1]
#     if r["Type"] == "string":
#         cols.append(StructField(r["Property"], StringType(), True))
#     elif r["Type"] == "float":
#         cols.append(StructField(r["Property"], FloatType(), True))
#     elif r["Type"] == "int" and "Timestamp" in r["Property"]:
#         cols.append(StructField(r["Property"], TimestampType(), True))
#     elif r["Type"] == "int" and "Timestamp" not in r["Property"]:
#         cols.append(StructField(r["Property"], IntegerType(), True))
#     elif r["Type"] == "boolean":
#         cols.append(StructField(r["Property"], BooleanType(), True))

# COMMAND ----------

# airbridge_api.raw_df[airbridge_api.raw_df["Campaign ID"].isna() == False]["Campaign ID"]

# COMMAND ----------

# spark.sql(f"delete from auto_report.airb_funble_mmp_raw where date_format(EventDatetime, 'yyyy-MM-dd') = '2022-09-25'")

# COMMAND ----------

# airbridge_api.raw_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Download CSV File Sample

# COMMAND ----------

# !wget -P /dbfs/ 'https://ab180-athena.s3.amazonaws.com/workgroup/airbridge-api/funble_2022-09-01_00%3A00%3A00%2B00%3A00-2022-09-26_00%3A00%3A00%2B00%3A00_jiho.lee%40ptbwa.com_20220926-0800_af0a352b-2b83-432c-8c0e-7699407fa0e6.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA5YP6HDUGSYI6BRNT%2F20220926%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Date=20220926T080107Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjED8aDmFwLW5vcnRoZWFzdC0xIkcwRQIhAKznyFjp1OK%2FlE05LaLUtTE0zpkfvS9pVQr2CUlNC9yMAiAf2FzSc3X5QbQBTQl7mmgmg%2BYk8adwpBcx2EyulSX5XyqbAwjp%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAMaDDk0NTk2MjgxODgyOSIMoNlQ0O6bTJcYob%2BLKu8Cggl%2BHVpmlyLQJ%2B%2F6oRMLcNO5qm3CBxWQ%2FZCYNicBHcCHrtLRcSLqoklPzlLz49Tw1t89y76oBet7T8j%2FGSVOPgdDYk3mRsStSyE51ukpD6A9jRZFA5FivEfJN7a3olZIGwCYDVjOhZF9AQuVhXrj4nUDQB9ECf7X%2F5%2BkmZEAsJbC1KC9d39jrqJfJqNkGyEhQYBGaq%2F92uB0z5ig%2FjQut5uFbbpLJN7ie9xJ8HOVcE9B8KP8WVYnXPloHbY8udHVjJ849keyqfWd9wSQEa42OCrkwDmZ%2BtRFfhO1sn2TC8lv5UJfM7Oz%2BTb4dwswOMQUFEDyMjiCV0Z%2B46mxEvb%2BZxjJPoxMwEPg4uUa6LZFJnFlYdocoPcFFrcjG8wI9EQkr3fGlZLbVBtuM9D6Aw58pul9sqEfzo11rUXEeqOxLxCeI%2B%2FCkJ%2BPmhflPG2KU2nTxnCfiNrasAn3nsis4zktMZqzekvvhrZMwVZO%2BZC5%2BjCursWZBjqdAXXPDlyYPS6pUwx1i3NxVeilG8fQP9Xt0Lk25uusFRZWKSXXhSszomtGR12xFMJLA7%2ByPP440kcON1GSZSAuVfSAD0ULp5rsAB7sxrToRUJRLNAgIH5JrNFvuHlmXkWvmpXFTjEI9WBfPZW3dFYC5UpdhlD6TWf3Ucwy76oYrPuulxiGMD1Fc77h2Kwxp4Q1KaZqoLGCVmxkgNdlplU%3D&X-Amz-Signature=997d14df11d6593f6f72560cb3a5a0828ad93a134c46fcbc65f738f766737bcd'