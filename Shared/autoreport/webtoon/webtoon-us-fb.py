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
f_log = log4jLogger.LogManager.getLogger("FACEBOOK-MARKETING")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Packages
# MAGIC - pip install facebook-business

# COMMAND ----------

!python -m pip install AttrDict
!python -m pip install facebook-business

# COMMAND ----------

import pickle
import os
import json
import requests
import sys

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi

from collections import defaultdict
from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from abc import ABC, abstractmethod
from typing import *
from pyspark.sql.functions import *

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



# COMMAND ----------

# since_date = "2023-01-11" 
# until_date = "2023-01-15"

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
    ADSETS = "id,name,status,effective_status"
    ADS = "id,name,status,effective_status"
#     ADS = "id,name,creative{id,name}"
    ADCREATIVES = "id,name,body"
    CAMPAIGNS_INSIGHTS = ','.join([
                "campaign_id",
                "campaign_name",
                "adset_id",
                "adset_name",
                "ad_id",
                "ad_name",
                "date_start",
                "impressions",
                "actions",
                "conversions",
                "spend",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ])
    
    ADSETS_INSIGHTS = ','.join([
                "campaign_id",
                "campaign_name",
                "adset_id",
                "adset_name",
                "ad_id",
                "ad_name",
                "date_start",
                "impressions",
                "actions",
                "conversions",
                "spend",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ])
    
    ADS_INSIGHTS = ','.join([
                "campaign_id",
                "campaign_name",
                "adset_id",
                "adset_name",
                "ad_id",
                "ad_name",
                "date_start",
                "impressions",
                "actions",
                "conversions",
                "spend",
                "video_p25_watched_actions",
                "video_p50_watched_actions",
                "video_p75_watched_actions",
                "video_p100_watched_actions",
                "video_play_actions"
            ])
#     CAMPAIGNS_INSIGHTS = ','.join([
#                 "campaign_id",
#                 "campaign_name",
#                 "adset_id",
#                 "adset_name",
#                 "ad_id",
#                 "ad_name",
#                 "date_start",
#                 "impressions",
#                 "clicks",
#                 "spend",
# #                 "linkClick"
#             ])
    
#     ADSETS_INSIGHTS = ','.join([
#                 "campaign_id",
#                 "campaign_name",
#                 "adset_id",
#                 "adset_name",
#                 "ad_id",
#                 "ad_name",
#                 "date_start",
#                 "impressions",
#                 "clicks",
#                 "spend",
# #                 "linkClick"
#             ])
    
#     ADS_INSIGHTS = ','.join([
#                 "campaign_id",
#                 "campaign_name",
#                 "adset_id",
#                 "adset_name",
#                 "ad_id",
#                 "ad_name",
#                 "date_start",
#                 "impressions",
#                 "clicks",
#                 "spend",
# #                 "linkClick"
#             ])
    
    # actions => link_click ref. action_breakdowns from api docs
#     CAMPAIGNS_INSIGHTS = ','.join([
#                 "campaign_id",
#                 "campaign_name",
#                 "adset_id",
#                 "adset_name",
#                 "ad_id",
#                 "ad_name",
#                 "date_start",
#                 "impressions",
#                 "actions",
#                 "conversions",
#                 "spend",
#                 "video_p25_watched_actions",
#                 "video_p50_watched_actions",
#                 "video_p75_watched_actions",
#                 "video_p100_watched_actions",
#                 "video_play_actions"
#             ])
    
class ParamGenerator(Enum):
    """
    A enum class for generating param parameter
    """
    
#     ADS = "effective_status=['ACTIVE']"
    CAMPAIGNS = "effective_status=['ACTIVE','PAUSED']&limit=1000"
#     ADSETS = "effective_status=['ACTIVE','PAUSED', 'ARCHIVED']&limit=200"
    ADSETS = "effective_status=['ACTIVE', 'PAUSED', 'PENDING_REVIEW', 'DISAPPROVED', 'PREAPPROVED', 'PENDING_BILLING_INFO', 'CAMPAIGN_PAUSED', 'ARCHIVED', 'ADSET_PAUSED', 'IN_PROCESS', 'WITH_ISSUES']&limit=1000"
    ADS = "ad.effective_status=['ACTIVE', 'PAUSED', 'PENDING_REVIEW', 'DISAPPROVED', 'PREAPPROVED', 'PENDING_BILLING_INFO', 'CAMPAIGN_PAUSED', 'ARCHIVED', 'ADSET_PAUSED', 'IN_PROCESS', 'WITH_ISSUES']&limit=1000"
#     ADS = "ad.effective_status=['ACTIVE','PAUSED', 'ARCHIVED']&limit=200"
    ADCREATIVES = f""
#     ADS_INSIGHTS = f"date_preset=yesterday&breakdowns={AdsInsights.Breakdowns.impression_device}&level=ad"
    
    
    # add limit param => default 25 daily
#     CAMPAIGNS_INSIGHTS = "time_range={since:'2022-08-30',until:'2022-08-30'}&breakdowns=impression_device&level=campaign&time_increment=1&limit=5000&filtering=[{field: 'ad.effective_status', operator:'IN', value:['ACTIVE', 'PAUSED', 'ARCHIVED', 'WITH_ISSUES', 'DELETED', 'CAMPAIGN_PAUSED']}]"
#     CAMPAIGNS_INSIGHTS = "date_preset=yesterday&breakdowns=impression_device&level=campaign&time_increment=1&limit=3000"
#     ADSETS_INSIGHTS = "date_preset=yesterday&breakdowns=impression_device&level=adset&time_increment=1&limit=3000"
#     ADS_INSIGHTS = "date_preset=yesterday&breakdowns=impression_device&level=ad&time_increment=1&limit=3000"
    CAMPAIGNS_INSIGHTS = "date_preset=yesterday&level=campaign&time_increment=1&limit=3000"
    ADSETS_INSIGHTS = "date_preset=yesterday&level=adset&time_increment=1&limit=3000"
    ADS_INSIGHTS = "date_preset=yesterday&level=ad&time_increment=1&limit=3000"
#     CAMPAIGNS_INSIGHTS = f"time_range={{since:'{since_date}',until:'{until_date}'}}&level=campaign&time_increment=1&limit=200"
#     ADSETS_INSIGHTS = f"time_range={{since:'{since_date}',until:'{until_date}'}}&level=adset&time_increment=1&limit=200"
#     ADS_INSIGHTS = f"time_ranges=[{{since:'2022-12-07',until:'2022-12-13'}},{{since:'2022-12-14',until:'2022-12-20'}},{{since:'2022-12-21',until:'2022-12-27'}}]&breakdowns=impression_device&level=ad&limit=300"
#     ADS_INSIGHTS = f"time_range={{since:'{since_date}',until:'{until_date}'}}&level=ad&time_increment=1&limit=3000"
    
    # time_range={since:'2022-12-14',until:'2022-12-14'}

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
        self.advertiser = "webtoon_us"
        self.customer_id = customer_id
        
        self.base_db = "auto_report"
       
        
        # todo add validation
#         todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        
        
#         self.domain = "ADSETS"
#         self.table_path = self.create_table_path()
#         self.get_adsets()
#         self.create_json_rdd()
#         self.delete_prev()
#         self.save_to_delta()

        self.domain = "ADS"
        self.table_path = self.create_table_path()
        self.get_ads()
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
            f_log.error(f"[FAIL-IS-CONFIG-EXIST]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting config as dict-based credentials
        """
        try:
            self._is_config_exist()
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            f_log.info(f"[CHECK-CONFIG]{self.config}")
        except Exception as e:
            f_log.error("[FAIL-SET-CONFIG]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for initiating facebook marketing api by access token
        """
        try:
            f_log.info(f"[CHECK-CONFIG-INFO]{self.config}")
        except Exception as e:
            f_log.error(f"[FAIL-SET-CLIENT]{self.config}")
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
            self.response = requests.get(url=request_url, params=request_params)
            
            result = self.response.json()
            self.check_result(domain, result)
            self.parse_result(domain, result)
        except Exception as e:
            f_log.error(f"[FAIL-GET-ADS]")
            
    def get_adsets(self):
        """
        A method for getting adgroup by {account id}
        """
        try:
            setattr(self, f"{self.domain.lower()}_json_rows", [])
            
            fields = self.get_fields_str(self.domain)
            params = self.get_params_str(self.domain)
            
            request_url = self.create_request_url(self.domain.lower())
            request_params = self.create_request_params(self.domain.lower(), fields, params)
            response = requests.get(url=request_url, params=request_params)
            
            result = response.json()
#             print(result)
            self.check_result(self.domain, result)
            self.parse_result(self.domain, result)
            
            # get stats
            self.get_insights(f"{self.domain}_INSIGHTS")
        except Exception as e:
            f_log.error(f"[FAIL-GET-ADSETS]")
            raise e
            
    def get_ads(self):
        """
        A method for getting ads
        """
        try:
            setattr(self, f"{self.domain.lower()}_json_rows", [])
#             domain = "ADS"
            fields = self.get_fields_str(self.domain)
            params = self.get_params_str(self.domain)
            
            request_url = self.create_request_url(self.domain.lower())
#             request_params = self.create_request_params(self.domain.lower(), fields)
            request_params = self.create_request_params(self.domain.lower(), fields, params)
            response = requests.get(url=request_url, params=request_params)
            
            result = response.json()
            self.check_result(self.domain, result)
            self.parse_result(self.domain, result)
            
            # get stats
            self.get_insights(f"{self.domain}_INSIGHTS")
        except Exception as e:
            f_log.error(f"[FAIL-GET-ADS]")
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
                if result["data"]:
                    self.create_json_rows_from_result(result)
        except Exception as e:
            f_log.error(f"[FAIL-GET-{domain}]")
            raise e
    
    def create_request_params(self, domain, fields, params=None):
        try:
            # (!) system user (AD Manager)
            request_params = {
                "fields": fields,
                "access_token": self.config["access_token"]
            }
    
            if params:
                for param in params.split("&"):
                    tmp = param.split("=")
                    request_params[tmp[0]] = tmp[1]
                
            print(f"[CHECK-PARAMS]{request_params}")
            return request_params
        except Exception as e:
            f_log.error(f"[FAIL-CREATE-REQUEST-PARAMS]{fields}/{params}")
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
            f_log.error(f"[FAIL-CREATE-REQUEST-URL]{target_id}/{domain}")
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
                "link_click",
                "omni_app_install",
                "post_engagement", 
                "page_engagement"
            ]
            
            for action_type in action_types:
                for row in values:
                    if action_type == row["action_type"]:
                        d[self.convert_snake_to_camel(action_type)] = row["value"]
        except Exception as e:
            f_log.error("[FAIL-PARSE-FIELD]")
            raise e
    
    def is_valid_dict(self, d):
        """
        A method for validating final json rows e.g. columns
        """
        try:
            cols =[
                "impressions",
                "spend",
                "clicks",
                "linkClick",
                "omniAppInstall",
                "postEngagement",
                "pageEngagement",
                "videoP25WatchedActions",
                "videoP50WatchedActions",
                "videoP75WatchedActions",
                "videoP100WatchedActions",
                "videoPlayActions",
                "Network_sub"
            ]

            for col in cols:
                if not d.get(col):
                    d[col] = "0"
        except Exception as e:
            f_log.error(f"[INVALID-JSON-ROWS]{d.keys()}")
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
#                 self.json_rows.append(json.dumps(d))
                getattr(self, f"{self.domain.lower()}_json_rows").append(json.dumps(d))
        except Exception as e:
            f_log.error("[FAIL-CREATE-JSON-ROWS-FROM-RESULT]")
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
            f_log.error(f"[FAIL-PARSE-RESULT]{domain}/{result}")
            raise e
    
    def check_result(self, domain, result):
        try:
            print(f"[{domain}-RESULT]")
            print(result)
            print()
            f_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e
            
    def get_fields_str(self, domain):
        """
        A method for creating fields string as response data
        """
        try:
            return getattr(FieldGenerator, domain).value
        except Exception as e:
            f_log.error(f"[FAIL-CREATE-FIELDS-STR]{domain}")
            raise e
            
    def get_params_str(self, domain):
        """
        A method for creating params string as request data
        """
        try:
            return getattr(ParamGenerator, domain).value
        except Exception as e:
            f_log.error(f"[FAIL-CREATE-PARAMS-STR]{domain}")
            raise e
    
    def create_json_rdd(self):
        """
        A method for creating json RDD before saving to delta
        """
        try:
#             jsonRdd = sc.parallelize(self.json_rows)
            jsonRdd = sc.parallelize(getattr(self, f"{self.domain.lower()}_json_rows"))
#             self.df = spark.read.json(jsonRdd)
            setattr(self, f"{self.domain.lower()}_df", spark.read.json(jsonRdd))
        except Exception as e:
            f_log.error(f"[FAIL-CREATE-JSON-RDD]{self.json_rows}")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
#         return f"{self.base_db}.{self.channel}_{self.advertiser}_{self.domain.lower()}_raw" # e.g. gad_series_adgroup_stats
        return f"{self.base_db}.{self.channel}_{self.advertiser}_ad_stats" # e.g. gad_series_adgroup_stats
    
    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
#             (self.df
            (getattr(self, f"{self.domain.lower()}_df")
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            f_log.error(f"[FAIL-SAVE-TO-DELTA]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous (default: yesterday) google ads naver series keyword stats
        """
        try:
            spark.sql(f"delete from {self.table_path} where dateStart = '{self.yesterday}'")
#             if self.domain == "ADSETS":  
#             spark.sql(f"delete from {self.table_path} where dateStart between '{since_date}' and '{until_date}'")
#             elif self.domain == "ADS":
#                 spark.sql(f"delete from {self.table_path} where dateStart='{since_date}' and dateStop='{until_date}'")
        except Exception as e:
            f_log.error(f"[FAIL-DELETE-PREV-GAD-SERIES-KEYWORD-STAT]TABLE-PATH:{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# webtoon us
customer_id = "325034845374215"
facebook_marketing_api = FacebookMarketingApi(customer_id)

# COMMAND ----------

try:
    facebook_marketing_api.proc_all()
except Exception as e:
    f_log.error("[FAIL-WEBTOON_US-FB]")
    raise e

# COMMAND ----------

facebook_marketing_api.config

# COMMAND ----------

# len(facebook_marketing_api.parsed_result["id"])
# facebook_marketing_api.parsed_result["id"]  

# COMMAND ----------

# json_rdd = sc.parallelize(facebook_marketing_api.json_rows)
# df = spark.read.json(json_rdd)
# df.display()

# COMMAND ----------

# df.write.format("delta").mode("append").saveAsTable("auto_report.tt_fb_series_ad_stats")

# COMMAND ----------

# df = spark.sql("select * from auto_report.fb_series_ad_stats where dateStart <= '2022-09-20'")

# COMMAND ----------

# df = df.withColumn("postEngagement", lit(None).cast("string"))
# df = df.withColumn("pageEngagement", lit(None).cast("string"))

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