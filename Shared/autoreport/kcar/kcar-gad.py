# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Google Ads API

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

from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from google.ads.googleads.client import GoogleAdsClient
from abc import ABC, abstractmethod
from typing import *
from google.protobuf.json_format import MessageToDict, MessageToJson

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
    log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

database

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
        assert value in ["pcar", "millie", "kakaopay"], f"[VALUE-ERROR]{value}"
        
    @staticmethod
    def is_valid_base_db(value):
        assert value in ["auto_report"], f"[VALUE-ERROR]{value}"
        
    @staticmethod
    def is_valid_channel(value):
        assert value in ["gad", "nsa"], f"[VALUE-ERROR]{value}"

# COMMAND ----------

args = AttrDict(dict())
widget_validator = WidgetValidator()

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
# MAGIC - Arguments
# MAGIC - Queries
# MAGIC - Types

# COMMAND ----------

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

class QueryGenerator(Enum):
    GET_GENERAL_CAMPAIGN = f"""
        SELECT
          campaign.id,
          campaign.name
        FROM campaign_criterion
        WHERE 
          campaign.advertising_channel_type IN ('DISPLAY')
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

# MAGIC %md
# MAGIC 
# MAGIC ### Google Ads API

# COMMAND ----------

# processor
class GoogleAdsApi(AutoReportAPI):
  
    def __init__(self, customer_id) -> None:
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        self.three_weeks_ago = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1) - timedelta(weeks=3), "%Y-%m-%d") # 어제의 3주전
        self.today_weekday = (datetime.now() + timedelta(hours=9)).weekday()
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "google_ads_api_client.pickle"
        self.json_rows = []
        self.channel = "gad"
        
        self.base_db = database
        self.advertiser = "kcar"
        self.customer_id = customer_id
        
        # todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        
        # (!) DISPLAY
        domain = "GENERAL"
        self.get_general_campaigns()
        self.get_general_ads()
        self.create_df(domain)
        self.delete_prev(domain)
        self.save_to_delta(domain)
        
        # (!) KEYWORD (=SEARCH)
        domain = "KEYWORD"
        self.get_keyword_campaigns()
        self.get_keyword_ads()
        self.create_df(domain)
        self.delete_prev(domain)
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
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for setting google ads api client from credentials
        """
        try:
            self.client = GoogleAdsClient.load_from_dict(self.config)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.config}")
            raise e
    
    def get_general_campaigns(self):
        """
        A method for getting campaign stat by {id}
        """
        try:
            self.set_service("DEFAULT")
            self.set_query("GET_GENERAL_CAMPAIGN")
            self.set_search_request()
            
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
    
    def get_general_ads(self):
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
                  ad_group_ad.ad.id,
                  ad_group_ad.ad.responsive_display_ad.long_headline,
                  ad_group_ad.ad.responsive_display_ad.descriptions,
                  metrics.cost_micros, 
                  metrics.clicks, 
                  metrics.impressions, 
                  metrics.conversions,
                  segments.device,
                  segments.date
                FROM ad_group_ad 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date BETWEEN '{self.three_weeks_ago}' AND '{self.yesterday}'
            """
            
            self.set_search_request()
            self.json_rows.clear()

            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    ad = row.ad_group_ad.ad
                    segments = row.segments
                    
                    self.json_rows.append(json.dumps({
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "adId": ad.id,
                        "adName": MessageToDict(ad.responsive_display_ad.long_headline)["text"],
                        "adDescriptions": MessageToDict(ad.responsive_display_ad.descriptions[0])["text"],
                        "costMicros": metrics.cost_micros,
                        "clicks": metrics.clicks,
                        "impressions": metrics.impressions,
                        "conversions": metrics.conversions,
                        "segmentDate": segments.date,
                        "segmentDevice": segments.device
                    }))
                    
            self.table_path = self.create_table_path("GENERAL")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
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
                  AND segments.date BETWEEN '{self.three_weeks_ago}' AND '{self.yesterday}'
            """
            
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
                    
            self.table_path = self.create_table_path("KEYWORD")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def set_search_request(self):
        """
        A method for setting final search request parameter
        """
        try:
            self.search_request = self.client.get_type("SearchGoogleAdsStreamRequest")
            self.search_request.customer_id = self.customer_id
            self.search_request.query = self.query
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_df(self, domain):
        """
        A method for creating json RDD before saving to delta
        """
        try:
            json_rdd = sc.parallelize(self.json_rows)
            setattr(self, f"{domain.lower()}_df", spark.read.json(json_rdd))
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.json_rows}")
            raise e
    
    def set_service(self, type):
        """
        A method for setting google ads api service type
        """
        try:
            service = getattr(ServiceGenerator, type).value
            self.ga_service = self.client.get_service(service)
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{type}")
            raise e
    
    def set_query(self, type):
        """
        A method for setting google ads api search query
        
        # query builder
        - https://developers.google.com/google-ads/api/docs/query/overview
        """
        try:
            self.query = getattr(QueryGenerator, type).value
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{type}")
            raise e
            
    def get_accessible_customer_list(self):
        """
        A method for getting accessible customer list based on client
        """
        try:
            customer_service = self.client.get_service("CustomerService")
            accessible_customers = customer_service.list_accessible_customers()
            
            result_total = len(accessible_customers.resource_names)
            resource_names = accessible_customers.resource_names
            for resource_name in resource_names:
                print(f'Customer resource name: "{resource_name}"')
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_table_path(self, domain):
        """
        A method for creating saving point path
        """
        return f"{self.base_db}.{self.channel}_{self.advertiser}_{domain.lower()}_stats"
    
    def save_to_delta(self, domain):
        """
        A method for saving stat data to delta
        """
        try:
            (getattr(self, f"{domain.lower()}_df")
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e
            
    def delete_prev(self, domain):
        """
        A method deleting previous google ads
        """
        try:
#             if self.today_weekday == 4: # 금요일일 때 워싱작업 진행
            spark.sql(f"delete from {self.table_path} where segmentDate between '{self.three_weeks_ago}' and '{self.yesterday}'")
#             else:
#                 spark.sql(f"delete from {self.table_path} where segmentDate = '{self.yesterday}'")
        except Exception as e:
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Processing

# COMMAND ----------

# KCAR
google_ads_api = GoogleAdsApi("7258345834")

try:
    google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-KCAR-GAD]")
    raise e

# COMMAND ----------

# google_ads_api.keyword_df.display()

# COMMAND ----------

# google_ads_api.general_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save to Delta Test

# COMMAND ----------

# table_name = "auto_report.gad_kakaopay_ad_stats"
# (google_ads_api.ad_df
#      .write
#      .mode("append")
#      .option("header", "true")
#      .saveAsTable(table_name))