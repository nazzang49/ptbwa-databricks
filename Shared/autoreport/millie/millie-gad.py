# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Google Ads API
# MAGIC - ChannelType
# MAGIC   - https://developers.google.com/google-ads/api/reference/rpc/v11/AdvertisingChannelTypeEnum.AdvertisingChannelType
# MAGIC - AdGroupCriterion
# MAGIC   - https://developers.google.com/google-ads/api/fields/v11/ad_group_criterion
# MAGIC - Criteria
# MAGIC   - App
# MAGIC     - Adgroup 기준
# MAGIC   - Video
# MAGIC     - Ad 기준

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Logging

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("GOOGLE-ADS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Load Packages

# COMMAND ----------

!python -m pip install --upgrade google-api-python-client
!python -m pip install oauth2client
!python -m pip install google-ads
!python -m pip install AttrDict
!python -m pip install grpcio-status

# COMMAND ----------

import pickle
import os
import json
import sys
import pprint

# from mysql_function import read_sql
from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from google.ads.googleads.client import GoogleAdsClient
from abc import ABC, abstractmethod
from typing import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Widget Validation
# MAGIC - Widget Args
# MAGIC - Default Args

# COMMAND ----------

class WidgetValidator:
     
    keys = [
        "channel",
        "advertiser",
        "config_name",
        "base_db"
    ]

    @staticmethod
    def is_valid_property(args):
        WidgetValidator.is_valid_config_name(args.config_name)
    
    @staticmethod
    def is_valid_widget(args):
        """
        A method for checking widget args
            1. existance
            2. validation
        """
        
        # existance
        try:
            for key in WidgetValidator.keys:
                args[key] = dbutils.widgets.get(key)
        except Exception as e:
            log.error(f"[FAIL-GET-INSERTED-ARGS]NOT-FOUND:{key}")
            raise e
        
        # validation
        WidgetValidator.is_valid_config_name(args.config_name)
        WidgetValidator.is_valid_advertiser(args.advertiser)
        WidgetValidator.is_valid_base_db(args.base_db)
 
    # each validation
    @staticmethod
    def is_valid_config_name(value):
        assert value in ["google_ads_api_client.pickle"], f"[VALUE-ERROR]{value}"
    
    @staticmethod
    def is_valid_advertiser(value):
        assert value in ["pcar", "millie", "series"], f"[VALUE-ERROR]{value}"
        
    @staticmethod
    def is_valid_base_db(value):
        assert value in ["auto_report"], f"[VALUE-ERROR]{value}"
        
    @staticmethod
    def is_valid_channel(value):
        assert value in ["gad", "nsa"], f"[VALUE-ERROR]{value}"
        
#     @staticmethod
#     def is_valid_advertiser(value):
#         assert value in ["pcar", "millie", "series"], f"[VALUE-ERROR]{value}"

#     @staticmethod
#     def is_valid_advertiser(value):
#         assert value in ["pcar", "millie", "series"], f"[VALUE-ERROR]{value}"

#     @staticmethod
#     def is_valid_advertiser(value):
#         assert value in ["pcar", "millie", "series"], f"[VALUE-ERROR]{value}"

# COMMAND ----------

# validation processing
args = AttrDict(dict())
widget_validator = WidgetValidator()
# widget_validator.is_valid_widget(args)

# COMMAND ----------

# print and log args
print(args)
log.info(f"[CHECK-ARGS]{args}")

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
# MAGIC ### AutoReportProcessor
# MAGIC - (TO_BE) Installed as Packages

# COMMAND ----------

class AutoReportProcessor():
    """
    A class as processor for each APIs
    """
    def __init__(self, auto_report_api) -> None:
        self.auto_report_api = self.is_valid_instance(auto_report_api)
    
    # todo add @property
    
    def is_valid_instance(self, auto_report_api: Type[AutoReportAPI]):
        if not isinstance(auto_report_api, AutoReportAPI):
            raise ValueError(f"[FAIL-CHECK-AUTOREPORT-API-INSTANCE]{type(auto_report_api)}")
        return auto_report_api
    
    def proc_all(self):
        
        self.auto_report_api._set_config()
        self.auto_report_api._set_client()
        
        

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Enum Control
# MAGIC - Argtments
# MAGIC - Queries
# MAGIC - Types

# COMMAND ----------

@unique
class ArgumentGenerator(Enum):
    CHANNELS = [
        "DISCOVERY",
        "DISPLAY",
        "HOTEL",
        "LOCAL",
        "LOCAL_SERVICES",
        "MULTI_CHANNEL",
        "PERFORMANCE_MAX",
        "SEARCH",
        "SHOPPING",
        "SMART",
        "UNKNOWN",
        "UNSPECIFIED",
        "VIDEO"
    ]

# COMMAND ----------

@unique
class QueryGenerator(Enum):
    
    # todo where condition
    # Multi Channel = APP
    
    GET_CAMPAIGN = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign_criterion.campaign,
          campaign_criterion.criterion_id,
          campaign_criterion.negative,
          campaign_criterion.type,
          campaign_criterion.keyword.text,
          campaign_criterion.keyword.match_type
        FROM campaign_criterion
        WHERE 
          campaign.advertising_channel_type IN ('MULTI_CHANNEL')
    """
    
    GET_KEYWORD_VIEW = f"""
        SELECT 
          keyword_view.resource_name, 
          metrics.impressions, 
          metrics.ctr, 
          metrics.cost_micros, 
          metrics.conversions, 
          metrics.clicks, 
          campaign.id, 
          campaign.labels, 
          campaign.name 
        FROM keyword_view 
        WHERE 
          campaign.advertising_channel_type IN ('SEARCH')
  """
    
    GET_KEYWORD_CAMPAIGN = f"""
        SELECT
          campaign.id,
          campaign.name
        FROM campaign_criterion
        WHERE 
          campaign.advertising_channel_type IN ('SEARCH')
    """

# COMMAND ----------

@unique
class ServiceGenerator(Enum):
    
    DEFAULT = "GoogleAdsService"

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

SEGMENT_DEVICE = [
    "0",
    "1",
    "Mobile Phones",
    "Tablets",
    "Computers",
    "5",
    "6",
    "7"
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Google Ads API

# COMMAND ----------

# processor
class GoogleAdsApi(AutoReportAPI):
  
    def __init__(self, customer_id) -> None:
        # todo move to super
        # load basic config params
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "google_ads_api_client.pickle"
        self.json_rows = []
        self.channel = "gad"
        
        self.base_db = "auto_report"
        #self.advertiser = "series"
#         self.advertiser = "pcar"
        self.advertiser = "millie_web"
#         self.table_path = self.create_table_path()
        self.customer_id = customer_id
        
        # todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        self.get_campaign_stat()
#         self.get_campaign()
#         self.get_adgroup()
#         self.get_adgroup_asset()
        self.get_keyword_campaigns()
    
        domain = "IMPRESSION_CLICK_COST"
        self.get_search_term_impression_click_cost()
        self.create_json_rdd(domain)
        self.delete_prev()
        self.save_to_delta(domain)
    
        domain = "CONVERSION_TYPE"
        self.get_search_term_conversion_type()
#         self.get_keyword_ads()
#         self.get_keyword_view()
        self.create_json_rdd(domain)
        self.delete_prev()
        self.save_to_delta(domain)

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
            log.error(f"[FAIL-IS-CONFIG-EXIST]{self.config_path}")
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
            log.error("[FAIL-SET-CONFIG]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for setting google ads api client from credentials
        """
        try:
            self.client = GoogleAdsClient.load_from_dict(self.config)
        except Exception as e:
            log.error(f"[FAIL-SET-CLIENT]{self.config}")
            raise e
            
    def get_keyword_campaigns(self):
        """
        A method for getting campaign stat by {id}
        """
        try:
            self.set_service("DEFAULT")
            self.set_query("GET_KEYWORD_CAMPAIGN")
            self.set_search_request()
            self.json_rows.clear()

            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    self.json_rows.append(json.dumps({
                        "campaignId": row.campaign.id,
                        "campaignName": row.campaign.name
                    }))
                    
            self.campaign_ids = list(set([json.loads(row)["campaignId"] for row in self.json_rows]))
            self.campaign_names = list(set([json.loads(row)["campaignName"] for row in self.json_rows]))
            self.campaign_ids = ', '.join(str(campaign_id) for campaign_id in self.campaign_ids)
            PtbwaUtils.check_result("CAMPAIGN_IDS", self.campaign_ids)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def get_search_term_impression_click_cost(self):
        """
        A method for getting ad stat by {campaign_id} during yesterday
        """
        try:
            self.set_service("DEFAULT")
            
#             if self.today_weekday == 4: # 금요일일 때 워싱작업 진행
            self.query = f"""
                SELECT 
                  search_term_view.search_term, 
                  campaign.id, 
                  ad_group.id, 
                  ad_group.name, 
                  campaign.name, 
                  metrics.cost_micros, 
                  metrics.clicks, 
                  metrics.impressions, 
                  segments.keyword.info.text,
                  segments.device,
                  segments.date
                FROM search_term_view 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date='{self.yesterday}'
            """
            # AND segments.date='{self.yesterday}'
        # AND segments.date between '2022-06-01' and '2022-11-01'
            self.set_search_request()
            self.json_rows.clear()

            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
#                     keyword = row.ad_group_criterion.keyword
                    segments = row.segments
                    search_term_view = row.search_term_view
                    
                    self.json_rows.append(json.dumps({
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "costMicros": metrics.cost_micros,
                        "clicks": metrics.clicks,
                        "impressions": metrics.impressions,
                        "segmentDate": segments.date,
                        "segmentDevice": SEGMENT_DEVICE[segments.device],
                        "segmentsKeywordInfoText" : segments.keyword.info.text,
                        "searchTerm" : search_term_view.search_term
                        
                    }))
                
            self.table_path = self.create_table_path("IMPRESSION_CLICK_COST")
                    
#             self.table_path = self.create_table_path("KEYWORD")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise 
    
    def get_search_term_conversion_type(self):
        """
        A method for getting ad stat by {campaign_id} during yesterday
        """
        try:
            self.set_service("DEFAULT")
            
#             if self.today_weekday == 4: # 금요일일 때 워싱작업 진행
            self.query = f"""
                SELECT 
                  ad_group.id, 
                  ad_group.name, 
                  campaign.id, 
                  campaign.name, 
                  search_term_view.search_term, 
                  segments.device, 
                  segments.date,
                  segments.keyword.info.text,
                  segments.conversion_action_name, 
                  metrics.all_conversions 
                FROM search_term_view 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date='{self.yesterday}'
                  AND segments.conversion_action_name IN ('(사용) 회원가입 (V3) (전체 웹사이트 데이터)', '프리미엄 구독 완료')
            """
    # AND segments.date between '2022-06-01' and '2022-10-31'
    #AND segments.date between '2022-06-01' and '2022-10-31'
    #AND segments.date='{self.yesterday}'
            
            self.set_search_request()
            self.json_rows.clear()

            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
#                     keyword = row.ad_group_criterion.keyword
                    segments = row.segments
                    search_term_view = row.search_term_view
                    
                    self.json_rows.append(json.dumps({
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "conversionActionName" : segments.conversion_action_name, 
                        "segmentDate": segments.date,
                        "segmentDevice": SEGMENT_DEVICE[segments.device],
                        "segmentsKeywordInfoText" : segments.keyword.info.text,
                        "searchTerm" : search_term_view.search_term,
                        "metricsAllConversions" : metrics.all_conversions 
                        
                    }))
                
            self.table_path = self.create_table_path("CONVERSION_TYPE")
                    
#             self.table_path = self.create_table_path("KEYWORD")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise 
            
    def get_keyword_ads(self):
        """
        A method for getting ad stat by {campaign_id} during yesterday
        """
        try:
            self.set_service("DEFAULT")
            
#             if self.today_weekday == 4: # 금요일일 때 워싱작업 진행
            self.query = f"""
                SELECT 
                  campaign.id, 
                  campaign.name, 
                  ad_group.id, 
                  ad_group.name, 
                  metrics.cost_micros, 
                  metrics.clicks, 
                  metrics.impressions, 
                  metrics.conversions,
                  ad_group_criterion.keyword.text,
                  segments.device,
                  segments.date
                FROM keyword_view 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date='{self.yesterday}'
            """
#             else:
#                 self.query = f"""
#                     SELECT 
#                       campaign.id, 
#                       campaign.name, 
#                       ad_group.id, 
#                       ad_group.name, 
#                       metrics.cost_micros, 
#                       metrics.clicks, 
#                       metrics.impressions, 
#                       metrics.conversions,
#                       ad_group_criterion.keyword.text,
#                       segments.device,
#                       segments.date
#                     FROM keyword_view 
#                     WHERE 
#                       campaign.id IN ({self.campaign_ids})
#                       AND segments.date DURING YESTERDAY
#                 """
            
            self.set_search_request()
            self.json_rows.clear()

            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    keyword = row.ad_group_criterion.keyword
                    segments = row.segments
                    
                    self.json_rows.append(json.dumps({
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "keyword": keyword.text,
                        "costMicros": metrics.cost_micros,
                        "clicks": metrics.clicks,
                        "impressions": metrics.impressions,
                        "conversions": metrics.conversions,
                        "segmentDate": segments.date,
                        "segmentDevice": segments.device
                    }))
                    
#             self.table_path = self.create_table_path("KEYWORD")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
#     def get_keyword_view(self):
#         """
#         A method for getting keyword view based on ad stat
#         """
#         try:
#             self.set_service("DEFAULT")
# #             self.set_query("GET_KEYWORD_VIEW")
# #             self.set_search_request()
            
#             # todo tmp
#             self.query = f"""
#                 SELECT 
#                   segments.date, 
#                   keyword_view.resource_name, 
#                   metrics.impressions, 
#                   metrics.ctr, 
#                   metrics.cost_micros, 
#                   metrics.conversions, 
#                   metrics.clicks, 
#                   campaign.id, 
#                   campaign.labels, 
#                   campaign.name,
#                   campaign.advertising_channel_type,
#                   ad_group.id,
#                   ad_group.name,
#                   ad_group_criterion.criterion_id,
#                   ad_group_criterion.keyword.text,
#                   ad_group_criterion.keyword.match_type
#                 FROM keyword_view 
#                 WHERE 
#                   campaign.id in ({self.campaign_ids})
#                   AND segments.date BETWEEN '2022-06-01' AND '{self.yesterday}'
#             """
            
# #             self.set_search_request()
#             stream = self.ga_service.search_stream(self.search_request)
#             self.json_rows.clear() # clear previous rows
#             for batch in stream:
#                 for row in batch.results:
#                     campaign = row.campaign
#                     ad_group = row.ad_group
#                     criterion = row.ad_group_criterion
#                     metrics = row.metrics
#                     segments = row.segments
                    
#                     self.json_rows.append(json.dumps({
#                         "segmentsDate" : segments.date, 
#                         "campaignId": campaign.id,
#                         "campaignName": campaign.name,
#                         "adgroupId": ad_group.id,
#                         "adgroupName": ad_group.name,
#                         "keywordId": criterion.criterion_id,
#                         "keywordName": criterion.keyword.text,
#                         "keywordMatchType": criterion.keyword.match_type,
#                         "costMicros": metrics.cost_micros,
#                         "clicks": metrics.clicks,
#                         "conversions": metrics.conversions,
#                         "impressions": metrics.impressions,
#                         "ctr": metrics.ctr,
#                     }))
                    
#                     print("[CHECK-ROWS-FROM-RESULT]")
#                     print(campaign.id)
#                     print(campaign.name)
#                     print(campaign.advertising_channel_type)
                    
#                     print(ad_group.id)
#                     print(ad_group.name)

#                     print(criterion.criterion_id)
#                     print(criterion.keyword.text)
#                     print(criterion.keyword.match_type)
                    
#                     print(metrics.ctr)
#                     print(metrics.impressions)
#                     print(metrics.clicks)
#                     print(metrics.cost_micros)
#                     print(metrics.conversions)
#                     print()
#         except Exception as e:
#             log.error("[FAIL-GET-KEYWORD-VIEW]")
#             raise e
    
    def get_campaign_stat(self):
        """
        A method for getting campaign stat by {id}
        """
        try:
            self.set_service("DEFAULT")
            self.set_query("GET_CAMPAIGN")
            self.set_search_request()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    # todo changeable
                    self.json_rows.append(json.dumps({
                        "campaignId": row.campaign.id,
                        "campaignName": row.campaign.name
                    }))
                    print("[CHECK-ROWS-FROM-RESULT]")
                    print(row.campaign.id)
                    print(row.campaign.name)
                    print(row.campaign_criterion.campaign)
                    print(row.campaign_criterion.keyword.text)
                    print()
            
            # retrieve campaign ids
            self.campaign_ids = list(set([json.loads(row)["campaignId"] for row in self.json_rows]))
            self.campaign_names = list(set([json.loads(row)["campaignName"] for row in self.json_rows]))
            self.campaign_ids = ', '.join(str(campaign_id) for campaign_id in self.campaign_ids)
            
            print('======================= CAMPAIGN IDS =======================')
            print(self.campaign_ids)
        except Exception as e:
            log.error("[FAIL-GET-CAMPAIGN-STAT]")
            raise e

    def get_campaign(self):
        try:
            self.set_service("DEFAULT")            
            self.query = """
            SELECT 
              campaign.id, 
              campaign.name, 
              segments.date, 
              campaign.optimization_score, 
              campaign.advertising_channel_type, 
              metrics.average_cpv, 
              metrics.average_cpm, 
              metrics.average_cpe, 
              metrics.average_cpc, 
              metrics.average_cost, 
              metrics.clicks, 
              metrics.conversions, 
              metrics.cost_per_conversion,
              metrics.interactions, 
              metrics.interaction_rate, 
              metrics.ctr, 
              metrics.cost_micros, 
              metrics.active_view_ctr, 
              metrics.active_view_cpm, 
              metrics.active_view_impressions,
              metrics.conversions_from_interactions_rate,
              metrics.impressions
            FROM campaign 
            WHERE 
              segments.date BETWEEN '2022-06-01' AND '2022-09-12'
            """
            
            self.set_search_request()
            self.json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
#                     print(row)
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    
                    self.json_rows.append(json.dumps({
                      "segmentsDate" : segments.date,
                      "campaignID": campaign.id, 
                      "campaignName" : campaign.name, 
                      "campaignAdvertisingChannelType" : campaign.advertising_channel_type,
#                       "campaignCampaignBudget" : campaign.campaign_budget, 
                      "campaignOptimizationScore" : campaign.optimization_score, 
                      "metricsAverageCPV" : metrics.average_cpv, 
                      "metricsAverageCPM" : metrics.average_cpm, 
                      "metricsAverageCPE" : metrics.average_cpe, 
                      "metricsAverageCPC" : metrics.average_cpc, 
                      "metricsAverageCost" : metrics.average_cost, 
                      "metricsClicks" : metrics.clicks, 
                      "metricsConversions" : metrics.conversions, 
                      "metricsCostPerConversion" : metrics.cost_per_conversion,
#                       "metricsAllConversions" : metrics.all_conversions, 
                      "metricsInteractions" : metrics.interactions, 
                      "metricsInteractionRate" : metrics.interaction_rate, 
                      "metricsImpressions" : metrics.impressions,
                      "metricsCTR" : metrics.ctr, 
                      "metricsCostMicors" : metrics.cost_micros, 
                      "metricsActiveViewCTR" : metrics.active_view_ctr, 
                      "metricsActiveViewCPM" : metrics.active_view_cpm,
                      "metricsActiveViewImpressions" : metrics.active_view_impressions,
                      "metricsConversionFromInteractionsRate" : metrics.conversions_from_interactions_rate 
                    }))
        except Exception as e:
            log.error("[FAIL-GET-CAMPAIGN-STAT]")
            raise e
    
    def get_adgroup(self):
        """
        A method for getting adgroup stat as report
        
        https://developers.google.com/google-ads/api/fields/v11/ad_group_query_builder
        """
        try:
            self.set_service("DEFAULT")
#             self.set_query("GET_CAMPAIGN")
#             self.set_search_request()
#             self.query = f"""
#             SELECT 
#                 segments.date,
#                 ad_group.id,
#                 ad_group.name, 
#                 ad_group.status, 
#                 ad_group.type, 
#                 ad_group.resource_name,
#                 segments.ad_network_type, 
#                 segments.device, 
#                 campaign.id,
#                 campaign.name
#             FROM ad_group_ad 
#             WHERE 
#                 segments.date BETWEEN '2022-06-01' AND '2022-09-12'
                
#              """
            self.query = f"""
            SELECT 
              metrics.all_conversions_value, 
              metrics.all_conversions, 
              metrics.cost_per_all_conversions, 
              metrics.cost_per_conversion, 
              metrics.conversions_value, 
              metrics.cost_micros, 
              metrics.clicks, 
              metrics.conversions, 
              metrics.cross_device_conversions, 
              metrics.impressions, 
              metrics.interactions, 
              metrics.value_per_conversion, 
              segments.ad_network_type, 
              segments.date, 
              segments.device, 
              campaign.id,
              campaign.name, 
              ad_group.id, 
              ad_group.name, 
              ad_group.status, 
              ad_group.type, 
              ad_group.resource_name 
            FROM ad_group_ad 
            WHERE 
              segments.date BETWEEN '2022-06-01' AND '2022-09-18'
            """
#               campaign.id IN ({self.campaign_ids})
#               AND segments.date BETWEEN '2022-06-01' AND '2022-09-12'
#              """
#              self.query = f"""
#                 SELECT 
#                   segments.date,
#                   campaign.id, 
#                   campaign.name, 
#                   ad_group.id, 
#                   ad_group.name, 
#                   segments.ad_network_type,
#                   metrics.impressions,
#                   metrics.interactions,
#                   metrics.conversions, 
#                   metrics.cost_micros
#                 FROM ad_group 
#                 WHERE 
#                   campaign.id IN ({self.campaign_ids})
#                   AND segments.date BETWEEN '2022-06-01' AND '2022-09-12'
#             """
#                   AND segments.date DURING YESTERDAY
#             """
            
            
            self.set_search_request()
            self.json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
#                     print(row)
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    
                    self.json_rows.append(json.dumps({
                      "metricAllConversionsValue" : metrics.all_conversions_value, 
                      "metricAllConversions" : metrics.all_conversions, 
                      "metricsCostPerAllConversions" : metrics.cost_per_all_conversions, 
                      "metricsCostPerConversion" : metrics.cost_per_conversion, 
                      "metricsConversionsValue" : metrics.conversions_value, 
                      "metricsConstMicors" : metrics.cost_micros, 
                      "metricsClicks" : metrics.clicks, 
                      "metricsConversions" : metrics.conversions, 
                      "metricsCrossDeviceConversions" : metrics.cross_device_conversions, 
                      "metricsImpressions" : metrics.impressions, 
                      "metricsInteractions" : metrics.interactions, 
                      "metricsValuePerConversion" : metrics.value_per_conversion, 
                      "segmentsAdNetworkType" : segments.ad_network_type, 
                      "segmentsDate" : segments.date, 
                      "segmentsDevice" : segments.device, 
                      "campaignID" : campaign.id,
                      "campaignName" : campaign.name, 
                      "adgroupID" : adgroup.id, 
                      "adgroupName" : adgroup.name, 
                      "adgroupStatus" : adgroup.status, 
                      "adgroupType" : adgroup.type_, 
                      "adgroupResourceName" : adgroup.resource_name 
                    }))
                    
#                     self.json_rows.append(json.dumps({
#                         "networkType": segments.ad_network_type,
#                         "segmentsDate": segments.date,
#                         "campaignId": campaign.id,
#                         "campaignName": campaign.name,
#                         "adgroupId": adgroup.id,
#                         "adgroupName": adgroup.name,
#                         "impressions": metrics.impressions,
#                         "interactions": metrics.interactions,
#                         "conversions": metrics.conversions,
#                         "costMicros": metrics.cost_micros,
#                         #"customerName": "SERIES" if self.customer_id == "3269406390" else "SERIES Re-engagement"
#                         "customerName": "PCAR"
#                     }))
                    
                    print("[CHECK-ROWS-FROM-RESULT]")
                    print(campaign.id)
                    print(campaign.name)
                    
                    print(adgroup.id)
                    print(adgroup.name)
                    
                    print(metrics.impressions)
                    print(metrics.interactions)
                    print(metrics.conversions)
                    print(metrics.cost_micros)
                    print()
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-STAT]")
            raise e
            
    def get_adgroup_asset(self):
        try:
            self.set_service("DEFAULT")
            self.set_search_request()

            self.query = f"""
            SELECT 
              ad_group.id, 
              ad_group.name, 
              asset.id, 
              asset.name, 
              metrics.all_conversions,
              metrics.all_conversions_value,
              metrics.conversions_value,
              metrics.conversions,
              metrics.impressions, 
              metrics.clicks, 
              metrics.ctr ,
              campaign.id,
              segments.date,
              asset.type,
              segments.ad_network_type
            FROM ad_group_ad_asset_view 
            WHERE 
              segments.date BETWEEN '2022-06-01' AND '2022-09-12'
            """
#                 campaign.id IN ({self.campaign_ids})
#                 AND segments.date BETWEEN '2022-06-01' AND '2022-09-12'
#             """
#                 AND segments.date DURING YESTERDAY               
#              """

            self.set_search_request()
            self.json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    ad_group = row.ad_group
                    asset = row.asset
                    metrics = row.metrics
                    campaign = row.campaign
                    print(asset)
                    
                    self.json_rows.append(json.dumps({
                          "assetID" : asset.id,
                          "assetName" : asset.name, 
                          "assetType" : ASSET_TYPE[asset.type_],
                          "segments.ad_network_type" : segments.ad_network_type,
                          "adgroupName" : ad_group.name,
                          "metricConversions" : metrics.conversions,
                          "metricConversionsValue" : metrics.conversions_value,
                          "metricsAllConversionValue" : metrics.all_conversions_value, # 모든 전환 가치
                          "metricsImpressions" : metrics.impressions,  # 노출 수
                          "metricsClicks" : metrics.clicks,  # 클릭 수 
                          "metricsCTR" : metrics.ctr,# 클릭률
                          "segmentsDate" : segments.date, 
                          "campaignID" : campaign.id,
                    }))
#                     print("[CHECK-ROWS-FROM-RESULT]")

                    
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-ASSET-STAT]")
            raise e

    
    def get_adgroup_ad(self):
        """
        A method for getting ad stat by {campaign_id} during yesterday
        """
        try:
            self.set_service("DEFAULT")
#             self.set_query("GET_CAMPAIGN")
#             self.set_search_request()
            
            self.query = f"""
                SELECT 
                  campaign.id, 
                  campaign.name, 
                  ad_group.id, 
                  ad_group.name, 
                  metrics.cost_micros, 
                  metrics.clicks, 
                  metrics.interactions,
                  metrics.conversions, 
                  metrics.ctr, 
                  metrics.impressions, 
                  metrics.active_view_viewability,
                  ad_group_ad.ad.resource_name,
                  ad_group_ad.ad.id
                FROM ad_group_ad 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date BETWEEN '2022-06-01' AND '2022-09-12'
            """
#                   AND segments.date DURING YESTERDAY 
#             """
            
            self.set_search_request()
            
            self.json_rows.clear()
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    ad = row.ad_group_ad.ad
                    
                    self.json_rows.append(json.dumps({
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "averageCost": metrics.average_cost,
                        "clicks": metrics.clicks,
                        "conversions": metrics.conversions,
                        "impressions": metrics.impressions,
                        "ctr": metrics.ctr,
                        "activeViewViewability": metrics.active_view_viewability,
                        "adId": ad.id,
                        "adName": ad.resource_name,
                    }))
                    
                    print("[CHECK-ROWS-FROM-RESULT]")
                    print(campaign.id)
                    print(campaign.name)
                    
                    print(adgroup.id)
                    print(adgroup.name)
                    
                    print(metrics.clicks)
                    print(metrics.conversions)
                    print(metrics.ctr)
                    print(metrics.impressions)
                    print(metrics.active_view_viewability)
                    print()
        except Exception as e:
            log.error("[FAIL-GET-AD-STAT]")
            raise e
    
    def set_search_request(self):
        """
        A method for setting final search request parameter
        """
        # todo type condition
        try:
            self.search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
            self.search_request.customer_id = self.customer_id
            self.search_request.query = self.query
        except Exception as e:
            log.error("[FAIL-SET-CLIENT-SERVICE]")
            raise e
    
    def create_json_rdd(self, domain):
        """
        A method for creating json RDD before saving to delta
        """
        try:
            json_rdd = sc.parallelize(self.json_rows)
#             self.df = spark.read.json(jsonRdd)
            setattr(self, f"{domain.lower()}_df", spark.read.json(json_rdd))
        except Exception as e:
            log.error(f"[FAIL-CREATE-JSON-RDD]{self.json_rows}")
            raise e
    
    def set_service(self, type):
        """
        A method for setting google ads api service type
        """
        try:
            service = getattr(ServiceGenerator, type).value
            self.ga_service = self.client.get_service(service)
            log.info(f"[CHECK-SERVICE]{self.ga_service}")
        except Exception as e:
            log.error(f"[FAIL-GET-SERVICE]{type}")
            raise e
    
    def set_query(self, type):
        """
        A method for setting google ads api search query
        
        # query builder
        - https://developers.google.com/google-ads/api/docs/query/overview
        """
        try:
            self.query = getattr(QueryGenerator, type).value
            log.info(f"[CHECK-QUERY]{self.query}")
        except Exception as e:
            log.error(f"[FAIL-SET-QUERY]{type}")
            raise e
            
    def get_accessible_customer_list(self):
        """
        A method for getting accessible customer list based on client
        """
        try:
            customer_service = self.client.get_service("CustomerService")
            accessible_customers = customer_service.list_accessible_customers()
            
            result_total = len(accessible_customers.resource_names)
            log.info(f"[ACCESSIBLE-CUSTOMER-LIST-LENGTH]{result_total}")
            
            resource_names = accessible_customers.resource_names
            for resource_name in resource_names:
                print(f'Customer resource name: "{resource_name}"')
        except Exception as e:
            log.error("[FAIL-GET-ACCESSIBLE-CUSTOMERcreate_json_rdd-LIST]")
            raise e
    
    def create_table_path(self, domain):
        """
        A method for creating saving point path
        """
        # todo add campaign_type
#         return f"{self.base_db}.tt_{self.channel}_{self.advertiser}_adgroup_stats" # e.g. gad_series_adgroup_stats
#         return f"{self.base_db}.tt_{self.channel}_{self.advertiser}_adgroup_asset_stats" # e.g. gad_series_adgroup_stats
#         return f"{self.base_db}.tt_{self.channel}_{self.advertiser}_campaign_stats2" # e.g. gad_millie_web_adgroup_stats
#         return f"{self.base_db}.tt_{self.channel}_{self.advertiser}_adkeyword_stats" # e.g. gad_millie_web_adgroup_stats
        return f"{self.base_db}.{self.channel}_{self.advertiser}_keyword_{domain.lower()}_stats"
    
    def save_to_delta(self, domain):
        """
        A method for saving stat data to delta
        """
        try:
            self.tmp_df = getattr(self, f"{domain.lower()}_df").distinct()
            self.tmp_df.write.mode("append").saveAsTable(self.table_path)            
        
#             (getattr(self, f"{domain.lower()}_df")
#                      .write
#                      .mode("overwrite")
#                      .saveAsTable(self.table_path)
#             )
        except Exception as e:
            log.error(f"[FAIL-SAVE-TO-DELTA]TABLE-PATH:{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous google ads naver series keyword stats
        """
        try:
            # delete previous stat data
            #customerName = "SERIES" if self.customer_id == "3269406390" else "SERIES Re-engagement"
#             customerName = "PCAR"
            #spark.sql(f"delete from {self.table_path} where segmentsDate = '{self.yesterday}' and customerName = '{customerName}'")
#             spark.sql(f"delete from {self.table_path}")
            spark.sql(f"delete from {self.table_path} where segmentDate='{self.yesterday}'")
        except Exception as e:
            log.error(f"[FAIL-DELETE-PREV]TABLE-PATH:{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Processor Test
# MAGIC - Dependency Injection in Processor Class

# COMMAND ----------

# processor test
# auto_report_processor = AutoReportProcessor(GoogleAdsApi())
# auto_report_processor.proc_all()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### API Test
# MAGIC - MILLIE WEB

# COMMAND ----------

# MILLIE WEB
customer_id = "7794767705"
google_ads_api = GoogleAdsApi(customer_id)
try:
    google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-MILLIE-GAD]")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Campaign IDs used in WHERE
# MAGIC - Currently, "APP"

# COMMAND ----------

# google_ads_api.campaign_ids

# COMMAND ----------

# google_ads_api.campaign_names

# COMMAND ----------

# google_ads_api.json_rows

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Adroup Ad Stat

# COMMAND ----------

# google_ads_api.get_adgroup_ad()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Adgroup Stat

# COMMAND ----------

# google_ads_api.get_adgroup_asset()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Asset Stat

# COMMAND ----------

# google_ads_api.get_adgroup_asset()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Keyword Stat

# COMMAND ----------

# google_ads_api.get_keyword_ads()
# google_ads_api.get_keyword_view()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Campaign Stat

# COMMAND ----------

# google_ads_api.get_campaign()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Search term Stat

# COMMAND ----------

# google_ads_api.get_search_term_impression_click_cost()
# google_ads_api.get_search_term_conversion_type()

# COMMAND ----------

# google_ads_api.create_json_rdd()

# COMMAND ----------

# google_ads_api.df = google_ads_api.df.select("campaignId", "campaignName", "adgroupId", "adgroupName", "adId", "adName", "averageCost", "clicks", "conversions", "impressions", "ctr", "activeViewViewability")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "campaignId", "adgroupName", "costMicros", "campaignName", "conversions", "impressions", "interactions", "networkType", "customerName", "segmentsDate")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "adgroupName", "assetID", "assetName")
# google_ads_api.df = google_ads_api.df.select("*")

# COMMAND ----------

# google_ads_api.df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save to Delta Test

# COMMAND ----------

# google_ads_api.table_path

# COMMAND ----------

# stat_table_name = "auto_report.gad_millie_web_keyword_impression_click_cost_stats"
# stat_table_name = "auto_report.gad_millie_web_keyword_conversion_type_stats"
# stat_table_name = google_ads_api.table_path
# google_ads_api.df.write.mode("overwrite").option("header", "true").saveAsTable(stat_table_name)

# COMMAND ----------

# delete previous
# google_ads_api.delete_prev_gad_series_keyword_stats()