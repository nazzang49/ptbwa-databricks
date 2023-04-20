# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Naver Webtoon US Google
# MAGIC - Account
# MAGIC   - AC
# MAGIC   - ACe
# MAGIC - `2023-01-02` V1 @jinyoung.park

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("GOOGLE-ADS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install --upgrade google-api-python-client
!python -m pip install oauth2client
!python -m pip install google-ads
!python -m pip install AttrDict
!python -m pip install grpcio-status
!python -m pip install bs4 
!python -m pip install tqdm

# COMMAND ----------

import pickle
import os
import json
import requests

from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from google.ads.googleads.client import GoogleAdsClient
from abc import ABC, abstractmethod
from typing import *
from bs4 import BeautifulSoup as Soup
from tqdm import tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Widget Validation
# MAGIC - Widget Args
# MAGIC - Default Args

# COMMAND ----------

# class WidgetValidator:
     
#     keys = [
#         "channel",
#         "advertiser",
#         "config_name",
#         "base_db"
#     ]

#     @staticmethod
#     def is_valid_property(args):
#         WidgetValidator.is_valid_config_name(args.config_name)
    
#     @staticmethod
#     def is_valid_widget(args):
#         """
#         A method for checking widget args
#             1. existance
#             2. validation
#         """
#         try:
#             for key in WidgetValidator.keys:
#                 args[key] = dbutils.widgets.get(key)
#         except Exception as e:
#             log.error(f"[FAIL-GET-INSERTED-ARGS]NOT-FOUND:{key}")
#             raise e
        
#         WidgetValidator.is_valid_config_name(args.config_name)
#         WidgetValidator.is_valid_advertiser(args.advertiser)
#         WidgetValidator.is_valid_base_db(args.base_db)
        
#     @staticmethod
#     def is_valid_config_name(value):
#         assert value in ["google_ads_api_client.pickle"], f"[VALUE-ERROR]{value}"
    
#     @staticmethod
#     def is_valid_advertiser(value):
#         assert value in ["pcar", "millie", "series"], f"[VALUE-ERROR]{value}"
        
#     @staticmethod
#     def is_valid_base_db(value):
#         assert value in ["auto_report"], f"[VALUE-ERROR]{value}"
        
#     @staticmethod
#     def is_valid_channel(value):
#         assert value in ["gad", "nsa"], f"[VALUE-ERROR]{value}"

# COMMAND ----------

# args = AttrDict(dict())
# widget_validator = WidgetValidator()
# widget_validator.is_valid_widget(args)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Class

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
# MAGIC #### Enums
# MAGIC - Arguments
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
          campaign.advertising_channel_type IN ('MULTI_CHANNEL', 'DISCOVERY', 'VIDEO')
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
# MAGIC #### Extract Fields

# COMMAND ----------

PERFORMANCE_LEVEL = [
    "0",
    "-",
    "2",
    "Learning",
    "Low",
    "Good",
    "Best"
]

# COMMAND ----------

ASSET_TYPE = [
    "0",
    "1",
    "YouTube video",
    "3",
    "4",
    "Text",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
]

# COMMAND ----------

ASSET_FIELD_TYPE = [
    "0",
    "1",
    "Headline",
    "Description",
    "4",
    "5",
    "6",
    "YouTube video",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26"
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Main API
# MAGIC - Refs
# MAGIC   - https://developers.google.com/google-ads/api/fields/v11/ad_group_query_builder
# MAGIC   - https://developers.google.com/google-ads/api/docs/query/overview

# COMMAND ----------

class GoogleAdsApi(AutoReportAPI):
    
    def __init__(self, customer_id) -> None:
        self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "google_ads_api_client.pickle"
        self.json_rows = []
        self.orientation_json_rows = []
        self.channel = "gad"
        self.base_db = "auto_report"
        self.advertiser = "latam"
        self.customer_id = customer_id
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        self.get_campaign_stat()
        
        domain = "ADGROUP"
        self.get_adgroup()
        self.create_json_rdd(domain)
#         self.delete_prev(domain)
#         self.save_to_delta(domain)

#         domain = "ORIENTATION"
#         self.get_video_orientation()
#         self.create_json_rdd(domain)
#         self.save_to_delta(domain)
        
        domain = "ASSET"
        self.get_asset()
        self.create_json_rdd(domain)
#         self.delete_prev(domain)
#         self.save_to_delta(domain)

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
            
            stream = self.ga_service.search_stream(self.search_request)
            self.json_rows.clear()
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
                    self.json_rows.append(json.dumps({
                        "campaignId": row.campaign.id,
                        "campaignName": row.campaign.name
                    }))

            self.campaign_ids = list(set([json.loads(row)["campaignId"] for row in self.json_rows]))
            self.campaign_names = list(set([json.loads(row)["campaignName"] for row in self.json_rows]))
            self.campaign_ids = ', '.join(str(campaign_id) for campaign_id in self.campaign_ids)
        except Exception as e:
            log.error("[FAIL-GET-CAMPAIGN-STAT]")
            raise e
        
    def get_adgroup(self):
        """
        A method for getting adgroup stat as report
        """
        try:
            self.set_service("DEFAULT")
            self.query = f"""
            SELECT 
              ad_group.id, 
              ad_group.name, 
              ad_group.status, 
              ad_group.type, 
              ad_group.status,
              ad_group_ad.ad.name,
              ad_group_ad.ad.id,
              campaign.id,
              campaign.name, 
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
              metrics.video_views,
              segments.ad_network_type, 
              segments.date
            FROM ad_group_ad 
            WHERE 
              campaign.id IN ({self.campaign_ids})
              AND segments.date BETWEEN '2022-10-01' AND '2022-10-01'
              AND metrics.cost_micros > 0
            """
            
            # ========================== TEST (AC)
#             AND segments.date BETWEEN '2022-03-01' AND '2022-03-01'
            
            # ========================== PROD
#             AND segments.date='{self.yesterday}'
            
            
            self.set_search_request()
            self.json_rows.clear()
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    metrics = row.metrics
                    ad = row.ad_group_ad.ad
                    
                    self.json_rows.append(json.dumps({
                      "metricAllConversionsValue" : metrics.all_conversions_value, 
                      "metricAllConversions" : metrics.all_conversions, 
                      "metricsCostPerAllConversions" : metrics.cost_per_all_conversions, 
                      "metricsCostPerConversion" : metrics.cost_per_conversion, 
                      "metricsConversionsValue" : metrics.conversions_value, 
                      "metricsCostMicors" : int(round(float(metrics.cost_micros) * 0.000001)),
                      "metricsClicks" : metrics.clicks, 
                      "metricsConversions" : metrics.conversions, 
                      "metricsCrossDeviceConversions" : metrics.cross_device_conversions, 
                      "metricsImpressions" : metrics.impressions, 
                      "metricsInteractions" : metrics.interactions, 
                      "metricsValuePerConversion" : metrics.value_per_conversion,
                      "metricsVideoView" : metrics.video_views,
                      "segmentsAdNetworkType" : segments.ad_network_type, 
                      "segmentsDate" : segments.date, 
                      "campaignID" : campaign.id,
                      "campaignName" : campaign.name, 
                      "adgroupID" : adgroup.id, 
                      "adgroupName" : adgroup.name, 
                      "adgroupStatus" : adgroup.status, 
                      "adgroupType" : adgroup.type_, 
                      "adId": ad.id,
                      "adName": ad.name,
                      "customerName" : "AC" if self.customer_id == "1149659634" else "ACe"
                    }))
                    
            self.table_path = self.create_table_path("ADGROUP")
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-STAT]")
            raise e
            
    def get_video_orientation(self):
        """
        A method for getting youtube video related stats
        """
        try:
            self.set_service("DEFAULT")
            youtube_id_list = spark.sql("select youtubeVideoID from hive_metastore.auto_report.gad_latam_asset_orientation")
            youtube_id_list = youtube_id_list.select("youtubeVideoID").rdd.flatMap(lambda x: x).collect()
            
            self.query = f"""
                SELECT 
                    asset.youtube_video_asset.youtube_video_id
                FROM ad_group_ad_asset_view 
                WHERE 
                  campaign.id IN ({self.campaign_ids})
                  AND asset.youtube_video_asset.youtube_video_id IS NOT NULL
                  AND segments.date='{self.yesterday}'
            """
            self.set_search_request()
            self.json_rows.clear()
            youtube_id_set = set()
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    asset = row.asset
                    if asset.youtube_video_asset.youtube_video_id not in youtube_id_list:
                        youtube_id_set.add(asset.youtube_video_asset.youtube_video_id)
                        
            for youtube_id in tqdm(youtube_id_set):
                url = "https://www.youtube.com/watch?v="+youtube_id
                response = requests.get(url)
                soup = Soup(response.text, "html.parser")
                script_text = soup.select("html > body")[0].select("script")[0].text

                width_idx = script_text.index("width")
                start_width_idx = script_text[width_idx:].index(":")
                end_width_idx = script_text[width_idx:].index(",")
                width = int(script_text[width_idx+start_width_idx+1:width_idx+end_width_idx])

                height_idx = script_text.index("height")
                start_height_idx = script_text[height_idx:].index(":")
                end_height_idx = script_text[height_idx:].index(",")
                height = int(script_text[height_idx+start_height_idx+1:height_idx+end_height_idx])

                if width == height : orientation = "Square"
                elif width > height : orientation = "Landscape"
                else : orientation = "Portrait"

                self.json_rows.append(json.dumps({
                         "youtubeVideoID" : youtube_id,
                         "orientation" : orientation,
                         "width" : width,
                         "height" : height,
                    }))
            self.table_path = self.create_table_path("ORIENTATION")
        except Exception as e:
            log.error("[FAIL-GET-VIDEO_ORIENTATIONT]")
            raise e

    def get_asset(self):
        try:
            self.set_service("DEFAULT")
            self.set_search_request()
            self.query = f"""
                SELECT 
                  campaign.id,
                  campaign.name,
                  ad_group.id, 
                  ad_group.name, 
                  ad_group_ad_asset_view.enabled,
                  ad_group_ad_asset_view.performance_label,
                  ad_group_ad_asset_view.field_type,
                  asset.id, 
                  asset.name, 
                  asset.type,
                  asset.text_asset.text,
                  asset.image_asset.full_size.url,
                  asset.youtube_video_asset.youtube_video_id,
                  asset.youtube_video_asset.youtube_video_title,
                  metrics.all_conversions,
                  metrics.all_conversions_value,
                  metrics.conversions_value,
                  metrics.conversions,
                  metrics.impressions, 
                  metrics.clicks, 
                  metrics.ctr,
                  metrics.cost_micros,
                  segments.date,
                  segments.ad_network_type
                FROM ad_group_ad_asset_view 
                WHERE 
                  ad_group_ad_asset_view.enabled = True
                  AND segments.date BETWEEN '2022-10-01' AND '2022-10-01'
            """

            # ========================== TEST (AC)
#             AND segments.date BETWEEN '2022-03-01' AND '2022-03-01'
            
            # ========================== PROD
#             AND segments.date='{self.yesterday}'
            
            
            self.set_search_request()
            self.json_rows.clear()
            
            stream = self.ga_service.search_stream(self.search_request)
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    ad_group = row.ad_group
                    ad_group_ad = row.ad_group_ad
                    asset = row.asset
                    metrics = row.metrics
                    campaign = row.campaign
                    ad_group_ad_asset_view = row.ad_group_ad_asset_view
                                  
                    self.json_rows.append(json.dumps({
                          "segmentsDate" : segments.date, 
                          "campaignID" : campaign.id,
                          "campaignName" : campaign.name,
                          "adgroupID" : ad_group.id,
                          "adgroupName" : ad_group.name,
                          "assetID" : asset.id,
                          "assetName" : asset.name, 
                          "assetType" : ASSET_TYPE[asset.type_],
                          "assetFieldType" : ASSET_FIELD_TYPE[ad_group_ad_asset_view.field_type],
                          "assetText" : asset.text_asset.text,
                          "assetImageFullSizeURL" : asset.image_asset.full_size.url,
                          "assetYoutubeTitle" : asset.youtube_video_asset.youtube_video_title,
                          "assetYoutubeVideoURL" : "https://www.youtube.com/watch?v="+asset.youtube_video_asset.youtube_video_id if asset.type_==2 else "",
                          "assetYoutubeVideoID" : asset.youtube_video_asset.youtube_video_id,
                          "assetPerformanceLevel" : PERFORMANCE_LEVEL[ad_group_ad_asset_view.performance_label],
#                           "adgroupadassetviewFieldType" : ad_group_ad_asset_view.field_type,
#                           "adgroupadassetviewFieldTypeName" : ASSET_FIELD_TYPE[ad_group_ad_asset_view.field_type],
#                           "metricConversions" : metrics.conversions,
#                           "metricConversionsValue" : metrics.conversions_value,
                          "metricsAllConversionValue" : metrics.all_conversions_value,
                          "metricsConversions" : metrics.conversions,
                          "metricsClicks" : metrics.clicks,           # 클릭수
                          "metricsCTR" : metrics.ctr,                 # 클릭률
                          "metricsImpressions" : metrics.impressions, # 노출수
                          "metricsCostMicors" : int(round(float(metrics.cost_micros) * 0.000001)),
                          "segmentsAdNetworkType" : segments.ad_network_type, 
                          "segmentsDate" : segments.date,
                          "customerName" : "AC" if self.customer_id == "1149659634" else "ACe",
                    }))

            self.table_path = self.create_table_path("ASSET")
        except Exception as e:
            log.error("[FAIL-GET-ADGROUP-ASSET-STAT]")
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
    
    def create_table_path(self, domain):
        """
        A method for creating saving point path
        """
        if domain == "ORIENTATION":
            return f"{self.base_db}.{self.channel}_{self.advertiser}_us_asset_{domain.lower()}"
        else:
            return f"{self.base_db}.{self.channel}_{self.advertiser}_us_{domain.lower()}_stats"
    
    def save_to_delta(self, domain):
        """
        A method for saving stat data to delta
        """
        try:
#             if domain == "ASSET":
#                 all_orientaoitn_df = spark.sql("select youtubeVideoID, orientation from hive_metastore.auto_report.tt_gad_latam_asset_orientation")
#                 asset_orientation_join_df = google_ads_api.asset_df.join(all_orientaoitn_df, google_ads_api.asset_df.assetYoutubeVideoID == all_orientaoitn_df.youtubeVideoID, "left").select("*")
#                 asset_orientation_join_df.write.mode("append").option("header", "true").saveAsTable(self.table_path)
#             else:
            (getattr(self, f"{domain.lower()}_df")
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            log.error(f"[FAIL-SAVE-TO-DELTA]{self.table_path}")
            raise e
            
    def delete_prev(self, domain):
        """
        A method deleting previous google ads Naver LATAM US
        """
        try:
            if self.customer_id == "1149659634": 
                customerName = "AC"
            else: 
                customerName = "ACe"
            spark.sql(f"delete from {self.table_path} where segmentsDate='{self.yesterday}' and customerName='{customerName}'")
        except Exception as e:
            log.error(f"[FAIL-DELETE-PREV-GAD-SERIES-KEYWORD-STAT]TABLE-PATH:{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### API Test
# MAGIC - LATAM AC
# MAGIC - LATAM ACe

# COMMAND ----------

customer_id = "1149659634"
ac_google_ads_api = GoogleAdsApi(customer_id)

try:
    ac_google_ads_api.proc_all()
except Exception as e:
    log.error("[FAIL-LATAM(US)-GAD]")
    raise e

# COMMAND ----------

ac_google_ads_api.adgroup_df.display()

# COMMAND ----------

ac_google_ads_api.asset_df.display()

# COMMAND ----------

# ac_google_ads_api.json_rows

# COMMAND ----------

# customer_id = "2769707735"
# ace_google_ads_api = GoogleAdsApi(customer_id)

# try:
#     ace_google_ads_api.proc_all()
# except Exception as e:
#     log.error("[FAIL-LATAM(US)-GAD]")
#     raise e

# COMMAND ----------

# ace_google_ads_api.adgroup_df.display()

# COMMAND ----------

# ace_google_ads_api.asset_df.display()

# COMMAND ----------

# ace_google_ads_api.save_to_delta("ASSET")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Get Adgroup Ad Stat

# COMMAND ----------

# google_ads_api.get_adgroup()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Video Orientation

# COMMAND ----------

# all_orientaoitn_df = spark.sql("select youtubeVideoID, orientation from hive_metastore.auto_report.tt_gad_latam_asset_orientation")
# all_orientaoitn_df.display()

# COMMAND ----------

# google_ads_api.asset_df.join(all_orientaoitn_df, google_ads_api.asset_df.assetYoutubeVideoID==all_orientaoitn_df.youtubeVideoID, "left").select("*").display()# google_ads_api.asset_df.join(all_orientaoitn_df, google_ads_api.asset_df.assetYoutubeVideoID==all_orientaoitn_df.youtubeVideoID, "left").select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Asset Stat

# COMMAND ----------

# google_ads_api.get_asset()

# COMMAND ----------

# google_ads_api.create_json_rdd()

# COMMAND ----------

# google_ads_api.df = google_ads_api.df.select("campaignId", "campaignName", "adgroupId", "adgroupName", "adId", "adName", "averageCost", "clicks", "conversions", "impressions", "ctr", "activeViewViewability")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "campaignId", "adgroupName", "costMicros", "campaignName", "conversions", "impressions", "interactions", "networkType", "customerName", "segmentsDate")
#google_ads_api.df = google_ads_api.df.select("adgroupId", "adgroupName", "assetID", "assetName")
# google_ads_api.df = google_ads_api.df.select("*")
# google_ads_api.asset_df = google_ads_api.asset_df.select("*")
# google_ads_api.adgroup_df = google_ads_api.adgroup_df.select("*")
# google_ads_api.orientation_df = google_ads_api.orientation_df.select("*")

# COMMAND ----------

# google_ads_api.asset_df.display()
# google_ads_api.adgroup_df.display()
# google_ads_api.orientation_df.display()