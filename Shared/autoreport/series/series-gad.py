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

from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from google.ads.googleads.client import GoogleAdsClient
from abc import ABC, abstractmethod
from typing import *

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
            log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
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
    
    # (!) TEST
#     env = "dev"
except Exception as e:
    log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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
    log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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

# MAGIC %md
# MAGIC 
# MAGIC ### Google Ads API

# COMMAND ----------

# processor
class GoogleAdsApi(AutoReportAPI):
    
    def __init__(self, customer_id) -> None:
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = data_interval_end
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "google_ads_api_client.pickle"
        self.json_rows = []
        self.channel = "gad"
        
        self.base_db = database
        self.advertiser = "series"
        self.table_path = self.create_table_path()
        self.customer_id = customer_id
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        self.get_campaign_stat()
        self.get_adgroup()
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
                    
                    self.json_rows.append(json.dumps({
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
    
    def get_adgroup(self):
        """
        A method for getting adgroup stat as report
        
        https://developers.google.com/google-ads/api/fields/v11/ad_group_query_builder
        """
        try:
            self.set_service("DEFAULT")
#             self.set_query("GET_CAMPAIGN")
#             self.set_search_request()
    
            self.query = f"""
                SELECT 
                  segments.date,
                  campaign.id, 
                  campaign.name, 
                  ad_group.id, 
                  ad_group.name, 
                  segments.ad_network_type,
                  metrics.impressions,
                  metrics.interactions,
                  metrics.conversions, 
                  metrics.cost_micros
                FROM ad_group 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND segments.date = '{self.yesterday}'
            """
            
            self.set_search_request()
            self.json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    
                    self.json_rows.append(json.dumps({
                        "networkType": segments.ad_network_type,
                        "segmentsDate": segments.date,
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "impressions": metrics.impressions,
                        "interactions": metrics.interactions,
                        "conversions": metrics.conversions,
                        "costMicros": metrics.cost_micros,
                        "customerName": "SERIES" if self.customer_id == "3269406390" else "SERIES Re-engagement"
                    }))
                    
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
                  AND segments.date = '{self.yesterday}'
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
    
    def create_json_rdd(self):
        """
        A method for creating json RDD before saving to delta
        """
        try:
            jsonRdd = sc.parallelize(self.json_rows)
            self.df = spark.read.json(jsonRdd)
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
            (self.df
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
            customerName = "SERIES" if self.customer_id == "3269406390" else "SERIES Re-engagement"
            spark.sql(f"delete from {self.table_path} where segmentsDate = '{self.yesterday}' and customerName = '{customerName}'")
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
# MAGIC - SERIES
# MAGIC - SERIES Re-engagement

# COMMAND ----------

# SERIES
google_ads_api = GoogleAdsApi("3269406390")

try:
    google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-SERIES-GAD]")
    raise e

# COMMAND ----------

# SERIES Re
google_ads_api = GoogleAdsApi("6084574266")

try:
    google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-SERIES(RE)-GAD]")
    raise e

# COMMAND ----------

# PCAR
# customer_id = "5632527570"
# google_ads_api = GoogleAdsApi(customer_id)
# google_ads_api.proc_all()

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

# google_ads_api.get_adgroup()

# COMMAND ----------

# google_ads_api.create_json_rdd()

# COMMAND ----------

# google_ads_api.df = google_ads_api.df.select("campaignId", "campaignName", "adgroupId", "adgroupName", "adId", "adName", "averageCost", "clicks", "conversions", "impressions", "ctr", "activeViewViewability")

# COMMAND ----------

# google_ads_api.df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save to Delta Test

# COMMAND ----------

# stat_table_name = "auto_report.gad_series_adgroup_stats"
# google_ads_api.df.write.mode("overwrite").option("header", "true").saveAsTable(stat_table_name)

# COMMAND ----------

# delete previous
# google_ads_api.delete_prev_gad_series_keyword_stats()