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
import pyspark.sql.functions as sf

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

# COMMAND ----------

@unique
class ServiceGenerator(Enum):
    
    DEFAULT = "GoogleAdsService"

# COMMAND ----------

NETWORK_TYPE=[
    "0",
    "1",
    "Search",
    "Search",
    "Display",
    "Search",
    "Youtube",
    "7",
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
        self.campaign_json_rows = []
        self.adgroup_json_rows = []
        self.asset_json_rows = []
        self.channel = "gad"
        
        self.base_db = "auto_report"
        #self.advertiser = "series"
#         self.advertiser = "pcar"
        self.advertiser = "ministock"
#         self.campaign_table_path, self.adgroup_table_path, self.asset_table_path = self.create_table_path()
        self.table_path = self.create_table_path()
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
        
        domain = "ADGROUP"
        self.get_adgroup()
        self.create_json_rdd(domain)
#         self.get_asset()
#         self.get_keyword_view()
        domain = "INSTALL_CONVERSIONS"
        self.get_install_conversions()
        self.create_json_rdd(domain)
        
        self.join_table()
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
    
    def get_keyword_view(self):
        """
        A method for getting keyword view based on ad stat
        """
        try:
            self.set_service("DEFAULT")
            self.set_query("GET_KEYWORD_VIEW")
            self.set_search_request()
            
            # todo tmp
            self.query = f"""
                SELECT 
                  segments.date, 
                  keyword_view.resource_name, 
                  metrics.impressions, 
                  metrics.ctr, 
                  metrics.cost_micros, 
                  metrics.conversions, 
                  metrics.clicks, 
                  campaign.id, 
                  campaign.labels, 
                  campaign.name,
                  campaign.advertising_channel_type,
                  ad_group.id,
                  ad_group.name,
                  ad_group_criterion.criterion_id,
                  ad_group_criterion.keyword.text,
                  ad_group_criterion.keyword.match_type
                FROM keyword_view 
                WHERE 
                  campaign.id in ({self.campaign_ids})
                  AND segments.date BETWEEN '2022-06-01' AND '2022-09-18'
            """
            
#             self.set_search_request()
            stream = self.ga_service.search_stream(self.search_request)
            self.json_rows.clear() # clear previous rows
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    ad_group = row.ad_group
                    criterion = row.ad_group_criterion
                    metrics = row.metrics
                    segments = row.segments
                    
                    self.json_rows.append(json.dumps({
                        "segmentsDate" : segments.date, 
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": ad_group.id,
                        "adgroupName": ad_group.name,
                        "keywordId": criterion.criterion_id,
                        "keywordName": criterion.keyword.text,
                        "keywordMatchType": criterion.keyword.match_type,
                        "costMicros": metrics.cost_micros,
                        "clicks": metrics.clicks,
                        "conversions": metrics.conversions,
                        "impressions": metrics.impressions,
                        "ctr": metrics.ctr,
                    }))
                    
                    print("[CHECK-ROWS-FROM-RESULT]")
                    print(campaign.id)
                    print(campaign.name)
                    print(campaign.advertising_channel_type)
                    
                    print(ad_group.id)
                    print(ad_group.name)

                    print(criterion.criterion_id)
                    print(criterion.keyword.text)
                    print(criterion.keyword.match_type)
                    
                    print(metrics.ctr)
                    print(metrics.impressions)
                    print(metrics.clicks)
                    print(metrics.cost_micros)
                    print(metrics.conversions)
                    print()
        except Exception as e:
            log.error("[FAIL-GET-KEYWORD-VIEW]")
            raise e
    
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
            self.query = f"""
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
              segments.date BETWEEN '2022-06-01' AND '{self.yesterday}'
            """
            
            self.set_search_request()
            self.campaign_json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
#                     print(row)
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    
                    self.campaign_json_rows.append(json.dumps({
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
              ad_group.id, 
              ad_group.name, 
              ad_group.status, 
              ad_group.type, 
              ad_group.status,
              campaign.id,
              campaign.name, 
              campaign.advertising_channel_sub_type,
              campaign.status,
              metrics.cost_micros, 
              metrics.impressions, 
              metrics.interactions, 
              segments.ad_network_type, 
              segments.date
            FROM ad_group        
            WHERE 
              campaign.id IN ({self.campaign_ids})
              AND metrics.impressions > 0
              AND metrics.cost_micros > 0
              AND metrics.cost_micros > 0
              AND segments.date='{self.yesterday}'
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
#                       "metricsConversionsValue" : metrics.conversions_value, 
                      "metricsConstMicors" : int(round(float(metrics.cost_micros)*0.000001)),
#                       "metricsConversions" : metrics.conversions, 
                      "metricsInstallConversions" : metrics.biddable_app_install_conversions,
                      "metricsImpressions" : metrics.impressions, 
                      "metricsInteractions" : metrics.interactions, 
                      "segmentsAdNetworkType" : segments.ad_network_type, 
                      "segmentsAdNetworkTypeName" : NETWORK_TYPE[segments.ad_network_type], 
                      "segmentsDate" : segments.date, 
                      "segmentsDevice" : segments.device, 
                      "campaignID" : campaign.id,
                      "campaignName" : campaign.name, 
                      "campaignStatus" : campaign.status,
                      "campaignAdvertisingChannelSubType" : campaign.advertising_channel_sub_type,
                      "adgroupID" : adgroup.id, 
                      "adgroupName" : adgroup.name, 
                      "adgroupStatus" : adgroup.status, 
                      "adgroupType" : adgroup.type_, 
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
                    
#                     print("[CHECK-ROWS-FROM-RESULT]")
#                     print(campaign.id)
#                     print(campaign.name)
                    
#                     print(adgroup.id)
#                     print(adgroup.name)
                    
#                     print(metrics.impressions)
#                     print(metrics.interactions)
#                     print(metrics.conversions)
#                     print(metrics.cost_micros)
#                     print()
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-STAT]")
            raise e
            
    def get_install_conversions(self):
        """
        A method for getting adgroup stat as report
        
        https://developers.google.com/google-ads/api/fields/v11/ad_group_query_builder
        """
        try:
            self.set_service("DEFAULT")
#              """
            self.query = f"""
            SELECT 
              ad_group.id,
              ad_group_ad_asset_view.enabled,
              campaign.id,
              metrics.biddable_app_install_conversions,
              segments.ad_network_type, 
              segments.date
            FROM ad_group_ad_asset_view    
            WHERE 
                ad_group_ad_asset_view.enabled = TRUE
                AND segments.date='{self.yesterday}'
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
                      "metricsInstallConversions" : metrics.biddable_app_install_conversions,
                      "segmentsAdNetworkType" : segments.ad_network_type,
                      "segmentsDate" : segments.date, 
                      "campaignID" : campaign.id,
                      "adgroupID" : adgroup.id, 
                    }))
                    
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-STAT]")
            raise e
            
    def join_table(self):
        self.sum_install_df = self.install_conversions_df.groupBy(["campaignID", "adgroupID", "segmentsAdNetworkType", "segmentsDate"]).agg(sf.sum('metricsInstallConversions').alias("metricsInstallConversions"))
        self.sum_install_df = self.sum_install_df[self.sum_install_df["metricsInstallConversions"]>0]
#         self.sum_install_df.display()
        self.join_table = self.adgroup_df.join(self.sum_install_df, ['campaignID','adgroupID', 'segmentsAdNetworkType', 'segmentsDate'], "left").select(self.adgroup_df.segmentsDate, self.adgroup_df.campaignID, self.adgroup_df.adgroupID, self.adgroup_df.adgroupName, self.adgroup_df.adgroupStatus, self.adgroup_df.campaignAdvertisingChannelSubType, self.adgroup_df.campaignName, self.adgroup_df.campaignStatus, self.adgroup_df.metricsConstMicors, self.adgroup_df.metricsImpressions, self.sum_install_df.metricsInstallConversions, self.adgroup_df.segmentsAdNetworkType, self.adgroup_df.segmentsAdNetworkTypeName)
            
    def get_asset(self):
        try:
            self.set_service("DEFAULT")
            self.set_search_request()
            
            self.query = f"""
                SELECT 
                  ad_group.id, 
                  ad_group.name, 
                  metrics.all_conversions,
                  metrics.all_conversions_value,
                  metrics.conversions_value,
                  metrics.conversions,
                  metrics.impressions, 
                  metrics.clicks, 
                  metrics.ctr,
                  metrics.biddable_app_install_conversions,
                  metrics.biddable_app_post_install_conversions,
                  campaign.id,
                  campaign.name,
                  segments.date,
                  asset.type,
                  segments.ad_network_type
                 FROM ad_group_ad_asset_view
                 WHERE 
                    campaign.id IN ({self.campaign_ids})
                    AND ad_group_ad_asset_view.enabled = True
                    AND segments.date BETWEEN '2022-06-01' AND '{self.yesterday}'
                """
#                 campaign.id IN ({self.campaign_ids})
#                 AND segments.date BETWEEN '2022-06-01' AND '2022-09-12'
#             """
#                 AND segments.date DURING YESTERDAY               
#              """

            self.set_search_request()
            self.asset_json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    ad_group = row.ad_group
                    asset = row.asset
                    metrics = row.metrics
                    campaign = row.campaign
                    ad_group_ad = row.ad_group_ad
                    ad_group_ad_asset_view = row.ad_group_ad_asset_view
                    print(asset)
                    
                    self.asset_json_rows.append(json.dumps({
                          "assetID" : asset.id,
                          "assetName" : asset.name, 
                          "assetType" : asset.type_,
                          "assetFullSizeURL" : asset.image_asset.full_size.url,
                          "segmentsAdNetworkType" : segments.ad_network_type,
                          "adgroupID" : ad_group.id,
                          "adgroupName" : ad_group.name,
                          "approvalStatus" : ad_group_ad_asset_view.enabled,
                          "metricConversions" : metrics.conversions,
                          "metricConversionsValue" : metrics.conversions_value,
                          "metricsAllConversionValue" : metrics.all_conversions_value, # 모든 전환 가치
                          "metricsImpressions" : metrics.impressions,  # 노출 수
                          "metricsClicks" : metrics.clicks,  # 클릭 수 
                          "metricsCTR" : metrics.ctr,# 클릭률
                          "metricsBiddableAppInstallConversion" : metrics.biddable_app_install_conversions,
                          "metricsBiddableAppPostInstallConversion" : metrics.biddable_app_post_install_conversions,
                          "segmentsDate" : segments.date, 
                          "campaignID" : campaign.id,
                          "campaignName" : campaign.name,
#                           "approvalStatus" : ad_group_ad.policy_summary.approval_status,
                          "assetText" : asset.text_asset.text,
                          "assetYoutubeTitle" : asset.youtube_video_asset.youtube_video_title,
                          "assetYoutubeVideoURL" : "https://www.youtube.com/watch?v="+asset.youtube_video_asset.youtube_video_id if asset.youtube_video_asset.youtube_video_id else ""
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
            log.error("[FAIL-GET-ACCESSIBLE-CUSTOMER-LIST]")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
        # todo add campaign_type
        return f"{self.base_db}.{self.channel}_{self.advertiser}_adgroup_stats" # e.g. gad_series_adgroup_stats

    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
            self.join_table = self.join_table.distinct()
            (self.join_table
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            log.error(f"[FAIL-SAVE-TO-DELTA]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        
        A method deleting previous google ads naver series keyword stats
        """
        try:
            # delete previous stat data
            #spark.sql(f"delete from {self.table_path} where segmentsDate = '{self.yesterday}' and customerName = '{customerName}'")
            spark.sql(f"delete from {self.table_path} where segmentsDate = '{self.yesterday}'")
#             spark.sql(f"delete from {self.table_path}")
        except Exception as e:
            log.error(f"[FAIL-DELETE-PREV-GAD-SERIES-KEYWORD-STAT]TABLE-PATH:{self.table_path}")
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

# COMMAND ----------

# MINISTOCK
customer_id = "1319033301"
google_ads_api = GoogleAdsApi(customer_id)
try:
    google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-MINISTOCK-GAD]")
    raise e

# COMMAND ----------

# google_ads_api.install_conversions_df.display()

# COMMAND ----------

# sum_install_df = google_ads_api.install_conversions_df.groupBy(["campaignID", "adgroupID", "segmentsAdNetworkType", "segmentsDate"]).agg({"metricsInstallConversions" : "sum"})
# import pyspark.sql.functions as sf

# sum_install_df = google_ads_api.install_conversions_df.groupBy(["campaignID", "adgroupID", "segmentsAdNetworkType", "segmentsDate"]).agg(sf.sum('metricsInstallConversions').alias("metricsInstallConversions"))
# sum_install_df = sum_install_df[sum_install_df["metricsInstallConversions"]>0]
# sum_install_df.display()

# COMMAND ----------

# google_ads_api.adgroup_df.join(sum_install_df, ['campaignID','adgroupID', 'segmentsAdNetworkType', 'segmentsDate'], "left").select(google_ads_api.adgroup_df.segmentsDate, google_ads_api.adgroup_df.campaignID, google_ads_api.adgroup_df.adgroupID, google_ads_api.adgroup_df.adgroupName, google_ads_api.adgroup_df.adgroupStatus, google_ads_api.adgroup_df.campaignAdvertisingChannelSubType, google_ads_api.adgroup_df.campaignName, google_ads_api.adgroup_df.campaignStatus, google_ads_api.adgroup_df.metricsConstMicors, google_ads_api.adgroup_df.metricsImpressions, sum_install_df.metricsInstallConversions, google_ads_api.adgroup_df.segmentsAdNetworkType, google_ads_api.adgroup_df.segmentsAdNetworkTypeName).display()

# COMMAND ----------

# google_ads_api.join_table.display()

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
# google_ads_api.get_adgroup()

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
# MAGIC ### Get Campaign Stat

# COMMAND ----------

# google_ads_api.get_campaign()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get keyword Stat

# COMMAND ----------

# google_ads_api.get_keyword_view()

# COMMAND ----------

# google_ads_api.create_json_rdd()

# COMMAND ----------

# google_ads_api.df = google_ads_api.df.select("campaignId", "campaignName", "adgroupId", "adgroupName", "adId", "adName", "averageCost", "clicks", "conversions", "impressions", "ctr", "activeViewViewability")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "campaignId", "adgroupName", "costMicros", "campaignName", "conversions", "impressions", "interactions", "networkType", "customerName", "segmentsDate")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "adgroupName", "assetID", "assetName")
# google_ads_api.df = google_ads_api.adgroup_df.select("*")
# google_ads_api.df = google_ads_api.campaign_df.select("*")
# google_ads_api.df = google_ads_api.df.select("*")

# COMMAND ----------

# google_ads_api.df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save to Delta Test

# COMMAND ----------

#stat_table_name = "auto_report.tt_gad_pcar_adgroup_stats"
# stat_table_name = google_ads_api.asset_table_path

# stat_table_name = google_ads_api.campaign_table_path
# google_ads_api.df.write.mode("overwrite").option("header", "true").saveAsTable(stat_table_name)

# COMMAND ----------

# google_ads_api.campaign_df.write.mode("overwrite").option("header", "true").saveAsTable(google_ads_api.campaign_table_path)
# google_ads_api.adgroup_df.write.mode("overwrite").option("header", "true").saveAsTable(google_ads_api.adgroup_table_path)
# google_ads_api.asset_df.write.mode("overwrite").option("header", "true").saveAsTable(google_ads_api.asset_table_path)

# COMMAND ----------

# delete previous
# google_ads_api.delete_prev_gad_series_keyword_stats()

# COMMAND ----------

