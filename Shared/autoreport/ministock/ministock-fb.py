# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Facebook Marketing API
# MAGIC - Through Graph API Explorer
# MAGIC   - https://developers.facebook.com/tools/explorer/1790612047959861/?method=GET&path=me%3Ffields%3Did%2Cname&version=v14.0
# MAGIC     - API Request Test
# MAGIC     - Generate Access Token
# MAGIC - Marketing API Docs
# MAGIC   - https://developers.facebook.com/docs/marketing-apis/
# MAGIC - Wiki
# MAGIC   - https://ptbwa.atlassian.net/wiki/spaces/PTBWA/pages/10420260/auto-report+facebook+marketing+api

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Logging

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("FACEBOOK-MARKETING")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Packages
# MAGIC - pip install facebook-business

# COMMAND ----------

!python -m pip install AttrDict

# COMMAND ----------

import pickle
import os
import json
import requests
import sys

from collections import defaultdict
from datetime import datetime, timedelta
from pytz import timezone
from attrdict import AttrDict
from enum import Enum, unique
from abc import ABC, abstractmethod
from typing import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Get Params

# COMMAND ----------

env = "prod"

try:
    # (!) LIVE
    env = dbutils.widgets.get("env")
    
    # (!) TEST
#     env = "dev"
except Exception as e:
    log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

try:
    # (!) LIVE
    data_interval_end = dbutils.widgets.get("data_interval_end")
    
    # (!) TEST
#     data_interval_end = "2023-02-27 14:00:00" UTC+9
except Exception as e:
    log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

try:
    today = datetime.strptime(data_interval_end, "%Y-%m-%d %H:%M:%S")
except Exception as e:
    today = (datetime.now(timezone('Asia/Seoul')))
    
yesterday = datetime.strftime(today - timedelta(days=1), "%Y-%m-%d")

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
    A class as parent class inherited by channels e.g. google-ads
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
        "insights",
        "ads",
        "campaigns",
        "adsets",
        "adcreatives",
    ]
    
    INSIGHTS = "insights"
    ADS = "ads"
    ADSETS = "adsets"
    CAMPAIGNS = "campaigns"
    ADCREATIVES = "adcreatives"
    
class FieldGenerator(Enum):
    """
    A enum class for generating field parameter
    """
    CAMPAIGNS = "id,name,status,effective_status"
    ADS = "id,name,creative{id,name}"
    ADCREATIVES = "id,name,body"
    ADS_INSIGHTS = ','.join([
                "campaign_id",
                "campaign_name",
                "adset_id",
                "adset_name",
                "ad_id",
                "ad_name",
                "date_start",
                "impressions",
                "clicks",
                "conversions",
                "spend",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ])
    
    # actions => link_click ref. action_breakdowns from api docs
    CAMPAIGNS_INSIGHTS = ','.join([
                "campaign_id",
                "campaign_name",
                "adset_id",
                "adset_name",
                "ad_id",
                "ad_name",
                "date_start",
                "impressions",
                "reach",
                "clicks",
                "actions",
#                 "conversions",
                "spend",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ])
    
class ParamGenerator(Enum):
    
    # todo add other domains
    
    """
    A enum class for generating param parameter
    """
    ADS = "effective_status=['ACTIVE']"
    CAMPAIGNS = "effective_status=['ACTIVE','PAUSED']&limit=50"
    ADCREATIVES = f""
#     ADS_INSIGHTS = f"date_preset=yesterday&breakdowns={AdsInsights.Breakdowns.impression_device}&level=ad"
    
    # add limit param => default 25 daily
#     CAMPAIGNS_INSIGHTS = "time_range={since:'2022-11-19',until:'2022-11-21'}&breakdowns=impression_device&level=ad&time_increment=1&limit=5000"
#     CAMPAIGNS_INSIGHTS = "date_preset=yesterday&breakdowns=impression_device&level=ad&time_increment=1&limit=5000"
    CAMPAIGNS_INSIGHTS = f"time_range={{since:'{yesterday}',until:'{yesterday}'}}&breakdowns=impression_device&level=ad&time_increment=1&limit=5000"

# COMMAND ----------

# FieldGenerator.INSIGHTS.value
# ParamGenerator.INSIGHTS.value

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save Client File

# COMMAND ----------

# d = {'access_token': 'EAAGqdAK3ZAeYBAM8JS7wwkGdiuh4YLaZBjNPjPi4wYuRpmYWGItz1ZALd8wMZBnhZCIGoZAm5IKalyENHFDab5G9dWc8YexS6jVlDRZBOLNMxjBl9gVDBquI0Ip189ubteBXPuYOEdMZAGoXstZAVebW7GKgyXokFDFYvfT5XdlrC6P2SNdLj9bG5loj6mJL7pDhDlaqiyGiOSJOlz1G3ZB06gVqXvrl3Sw2kZD',
#  'app_secret': '0fbad918f81db39a0d52798b5eee9a9d',
#  'app_id': '468890215212518'}

# COMMAND ----------

# with open("/dbfs/FileStore/configs/facebook_marketing_api_client_1.pickle", "wb") as f:
#     pickle.dump(d, f)

# COMMAND ----------

# with open("/dbfs/FileStore/configs/facebook_marketing_api_client_1.pickle", "rb") as f:
#     config = pickle.load(f)

# COMMAND ----------

# config

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Facebook Marketing API
# MAGIC - By Graph API
# MAGIC - App Type
# MAGIC   - Business

# COMMAND ----------

class FacebookMarketingApi(AutoReportAPI):
    """
    A class for processing facebook marketing API procedures
    args:
        customer_id: account_id in facebook
    """
    def __init__(self, customer_id) -> None:
        self.json_rows = []
        self.ads = []
        
        self.base_url = "https://graph.facebook.com"
        self.api_version = "v15.0"
        
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "facebook_marketing_api_client_ptbwa.pickle"

        self.channel = "fb"
        self.advertiser = "ministock"
        self.customer_id = customer_id
        
        #self.base_db = "auto_report"
        self.base_db = database
        self.table_path = self.create_table_path()
        
        # todo add validation
        
#         todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        # (!) neccessary to check "2022-11-22"
#         self._refresh_token()
        
        self.get_campaigns()
        self.create_json_rdd()
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
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting config as dict-based credentials
        """
        try:
            self._is_config_exist()
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            log.info(f"[CHECK-CONFIG]{self.config}")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for initiating facebook marketing api by access token
        """
        try:
            log.info(f"[CHECK-CONFIG-INFO]{self.config}")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config}")
            raise e
            
    def _refresh_token(self):
        """
        A method for getting long-lived user access token e.g. max 60d
        period: every day
        """
        try:
            params = {
                'grant_type': 'fb_exchange_token',
                'client_id': '468890215212518', # (!) ptbwa1 app id
                'client_secret': '0fbad918f81db39a0d52798b5eee9a9d',
                'fb_exchange_token': self.config["access_token"],
            }

            response = requests.get('https://graph.facebook.com/v15.0/oauth/access_token', params=params)
            result = response.json()
            
            self.config["access_token"] = result["access_token"]
            self.config["token_type"] = result["token_type"]
            self.config["expires_in"] = result["expires_in"]
            print("NEW-CONFIG-INFO : ", self.config)
            log.info(f"[SUCCESS-{sys._getframe().f_code.co_name.upper()}]{self.config}")

            # (!) save new token
            with open(self.config_path, "wb") as f:
                pickle.dump(self.config, f)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config}")
            raise e
    
    def get_adcreatives(self):
        """
        A method for getting adcreatives
        """
        try:
            domain = "ADCREATIVES"
            fields = self.get_fields_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_params = self.create_request_params(domain.lower(), fields)
            response = requests.get(url=request_url, params=request_params)
            
            result = response.json()
            self.check_result(domain, result)
            self.parse_result(domain, result)
            print(self.parsed_result)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            
    def get_campaigns(self):
        """
        A method for getting campaigns by {account id}
        """
        try:
            domain = "CAMPAIGNS"
            fields = self.get_fields_str(domain)
            params = self.get_params_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_params = self.create_request_params(domain.lower(), fields, params)
            response = requests.get(url=request_url, params=request_params)
            
            result = response.json()
            self.check_result(domain, result)
            self.parse_result(domain, result)
            print(self.parsed_result)
            
            # get stats
            self.get_insights(f"{domain}_INSIGHTS")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_ads(self):
        """
        A method for getting ads
        """
        try:
            domain = "ADS"
            fields = self.get_fields_str(domain)
            
            request_url = self.create_request_url(domain.lower())
            request_params = self.create_request_params(domain.lower(), fields)
            response = requests.get(url=request_url, params=request_params)
            
            result = response.json()
            self.check_result(domain, result)
            self.parse_result(domain, result)
            print(self.parsed_result)
            
            # get stats
            self.get_insights(f"{domain}_INSIGHTS")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_insights(self, domain):
        """
        A method for getting ads insights by {ad_account_id} {campaign_id} {adset_id} {ad_id} (!) should be called after calling each domains
        """
        try:
            keys = list(self.parsed_result.keys())
            assert "id" in keys, f"[NOT-FOUND-ID-KEY]{keys}"
            
            fields = self.get_fields_str(domain)
            params = self.get_params_str(domain)
            insights = domain.split("_")[1].lower()
            request_params = self.create_request_params(insights, fields, params)

            for id in self.parsed_result["id"]:
                request_url = self.create_request_url(insights, id)
                response = requests.get(url=request_url, params=request_params)
                result = response.json()
                
                self.check_result(domain, result)
                
                # (!) if result exist
                if result.get("data"):
                    self.create_json_rows_from_result(result)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_request_params(self, domain, fields, params=None):
        """
        A method for creating request params just before API process
        """
        try:
            request_params = {
                "fields": fields,
                "access_token": self.config["access_token"]
#                 "access_token" : "EAAZAcjVsMlzUBAAKXIAw6XB6aV2Mq3C9JnpBNRQRgQZBhVW496737vUUx47PP9IfhZBvCWqqwGrTaJEgcXMNTm2YRuC80YKFztY5cVCAgxIYfjcV3Jpf6xt9amJP9ABltdyYxj6Qzlnvf5SbfPMy25UvhUMVyRJJ6v5vw6i1ufSQFmWMEzsYwlPWFFZA576XBZAyzKccjAWTf6uyETt7ROJK50VVG63kZD"
            }
            
            if params:
                for param in params.split("&"):
                    tmp = param.split("=")
                    request_params[tmp[0]] = tmp[1]
                
            print(f"[CHECK-PARAMS]{request_params}")
            return request_params
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{fields}/{params}")
            raise e
    
    def create_request_url(self, domain, id=None):
        """
        A method for creating api request url
            ref. facebook graph, marketing api docs
            e.g. domain = insights, campaigns, adsets, ads, etc.
        """
        try:
            assert domain in ArgumentGenerator.DOMAINS.value, f"[NOT-FOUND-DOMAINS]{domain}"
            
            target_id = id if id else f"act_{self.customer_id}"
            assert target_id is not None, "[NOT-FOUND-TARGET-ID]"
            
            request_url = f"{self.base_url}/{self.api_version}/{target_id}/{domain}"
            print(f"[CHECK-REQUEST-URL]{request_url}")
            return request_url
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{target_id}/{domain}")
            raise e
    
    def convert_snake_to_camel(self, value):
        """
        A method for converting snake to camel
        """
        return ''.join(w.title() if i != 0 else w for i, w in enumerate(value.split('_')))
    
    def parse_field(self, d, values):
        """
        A method for parsing nested field as converting to json rows
        """
        try:
            action_types = [
                "mobile_app_install",
            ]
            
            for action_type in action_types:
                for row in values:
                    if action_type == row["action_type"]:
                        d[self.convert_snake_to_camel(action_type)] = row["value"]
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def is_valid_dict(self, d):
        """
        A method for validating final json rows e.g. columns
        """
        try:
            cols =[
                "impressions",
                "clicks",
                "reach",
                "spend",
                "mobileAppInstall",
                "videoP25WatchedActions",
                "videoP50WatchedActions",
                "videoP75WatchedActions",
                "videoP100WatchedActions",
                "videoPlayActions"
            ]

            for col in cols:
                if not d.get(col):
                    d[col] = "0"
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{d.keys()}")
            raise e
    
    def create_json_rows_from_result(self, result):
        """
        A method for creating json rows from result e.g. insight
        """
        try:
            video_types = [
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ]
            
            # (!) 액션 자체가 없는 경우 존재
            for row in result["data"]:
                d = dict()
                for k, v in row.items():
                    if k == "actions":
                        self.parse_field(d, v)
                    elif k in video_types:
                        d[self.convert_snake_to_camel(k)] = v[0]["value"]
                    else:
                        d[self.convert_snake_to_camel(k)] = v
                self.is_valid_dict(d)
                self.json_rows.append(json.dumps(d))
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def parse_result(self, domain, result):
        """
        A method for parsing result based on domain e.g. data, paging, summary
        result: dict()
            data: list(dict())
        """
        try:
            self.parsed_result = defaultdict(list)
            for row in result["data"]:
                for k, v in row.items():
                    self.parsed_result[k].append(v)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]DOMAIN:{domain}/RESULT:{result}")
            raise e
    
    def check_result(self, domain, result):
        try:
            print(f"[{domain}-RESULT]")
            print(result)
            print()
            log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e
            
    def get_fields_str(self, domain):
        """
        A method for creating fields string as response data
        """
        try:
            return getattr(FieldGenerator, domain).value
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
            
    def get_params_str(self, domain):
        """
        A method for creating params string as request data
        """
        try:
            return getattr(ParamGenerator, domain).value
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
    
    def create_json_rdd(self):
        """
        A method for creating json RDD before saving to delta
        """
        try:
            jsonRdd = sc.parallelize(self.json_rows)
            self.df = spark.read.json(jsonRdd)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}-JSON-ROWS]{self.json_rows}")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
        return f"{self.base_db}.{self.channel}_{self.advertiser}_ad_stats"
    
    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
            (self.df
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous (default: yesterday) google ads ministock keyword stats
        """
        try:
            spark.sql(f"delete from {self.table_path} where dateStart = '{self.yesterday}'")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# MINISTOCK
customer_id = "3373854206030430"
facebook_marketing_api = FacebookMarketingApi(customer_id)

# COMMAND ----------

try:
    facebook_marketing_api.proc_all()
except Exception as e:
    log.error("[FAIL-MINISTOCK-FB]")
    raise e

# COMMAND ----------

# facebook_marketing_api.create_json_rdd()

# COMMAND ----------

# facebook_marketing_api.save_to_delta()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save to Delta Manually

# COMMAND ----------

# df.write.mode("append").saveAsTable("auto_report.fb_series_ad_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Generate App Secret Proof Value
# MAGIC - Used for creating System User

# COMMAND ----------

# import hashlib
# import hmac

# def genAppSecretProof(app_secret, access_token):
#     h = hmac.new(
#         app_secret.encode('utf-8'),
#         msg=access_token.encode('utf-8'),
#         digestmod=hashlib.sha256
#     )
#     return h.hexdigest()

# COMMAND ----------

# app_secret_proof = genAppSecretProof('a41d34cb54d3783aad6b076b7702916c', 'EAAZAcjVsMlzUBAKn6x4agNE9PKwOK0gZBoiZBC3ZBBNj5iLKVLUslWGGceCZBZCnDwApgWdoiFiLzQav7F0Ngjw14OCB6Wh7Dz6hlPm1N9ijbGs2vUQEEGxO1cPjJEtFWZBP0Iztf72lF3Y99E6I7PLoePaOZAYrPTG6jwCfwH7QCpTIXoZCRHBuMOTMlvMW0qyp4Ga1voUt4ZAFzPTUVojelxirZBdUTvkGlEZD')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Another Sample

# COMMAND ----------

# curl -G \
# -d "fields=insights{impressions}" \
# -d "access_token=<ACCESS_TOKEN>" \
# "https://graph.facebook.com/<API_VERSION>/<AD_ID>"

# COMMAND ----------

# curl -G \
# -d "sort=reach_descending" \
# -d "level=ad" \
# -d "fields=reach" \
# -d "access_token=<ACCESS_TOKEN>" \
# "https://graph.facebook.com/<API_VERSION>/<ADSET_ID>/insights"

# COMMAND ----------

# curl -G \  
# -d "fields=id,name,insights{unique_clicks,cpm,total_actions}" \
# -d "level=ad" \
# -d 'filtering=[{"field":"ad.adlabels","operator":"ANY", "value":["Label Name"]}]'  \
# -d 'time_range={"since":"2015-03-01","until":"2015-03-31"}' \
# -d "access_token=<ACCESS_TOKEN>" \
# "https://graph.facebook.com/<API_VERSION>/<AD_OBJECT_ID>/insights"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Extended Access Token Test
# MAGIC - https://developers.facebook.com/docs/facebook-login/guides/access-tokens/get-long-lived
# MAGIC - https://developers.facebook.com/docs/facebook-login/guides/%20access-tokens/debugging

# COMMAND ----------

# import requests

# params = {
#     'grant_type': 'fb_exchange_token',
#     'client_id': '468890215212518',
#     'client_secret': '0fbad918f81db39a0d52798b5eee9a9d',
#     'fb_exchange_token': 'EAAGqdAK3ZAeYBALZAn8HRfK0Wm5RVKMrhIjf7cHYITemUAbi8zxRkWQm1HoM6iOqSrGSqWQ81OJ4F1ZAZC7ndtK1qAigfPWDZBIRrPVytcun38WWHixmi5DrycE8alRTRpjBJ21asXFk6QJKenazZBgQc6DyggHEgZCgyXT6v3OuSabQvI97vsJ',
# }

# response = requests.get('https://graph.facebook.com/v15.0/oauth/access_token', params=params)

# COMMAND ----------

# response.json()