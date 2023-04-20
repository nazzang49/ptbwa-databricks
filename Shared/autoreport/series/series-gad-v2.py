# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Series Google API
# MAGIC - Campaign
# MAGIC   - Display
# MAGIC   - Video (Youtube)
# MAGIC - References
# MAGIC   - [Report](https://ads.google.com/aw/reporting/reporteditor/list?ocid=284034859&euid=644738079&__u=1894724071&uscid=284034859&__c=3535590291&authuser=0&subid=ALL-ko-et-g-aw-c-home-awhp_xin1_signin%21o2-awhp-hv-01-22)
# MAGIC     - Display
# MAGIC     - Video (Youtube)
# MAGIC - `2023-01-13` V1 | Init_캠페인 유형 추가 작업 | @jinyoung.park

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
w_log = log4jLogger.LogManager.getLogger("SERIES-GOOGLE(V2)")

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
!python -m pip install validators

# COMMAND ----------

import pickle
import os
import json
import pprint
import time
import pandas as pd
import sys
import requests
import validators
import re

from pyspark.sql.types import *
from pyspark.sql.functions import *

from ptbwa_utils import *

from urllib.parse import urljoin, urlunparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from abc import ABC, abstractmethod
from typing import *
from collections import defaultdict
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.json_format import MessageToDict, MessageToJson

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
    w_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

env

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### {data_interval_end}

# COMMAND ----------

data_interval_end = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=1), "%Y-%m-%d")
today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")

# COMMAND ----------

try:
    # (!) LIVE
    data_interval_end = AutoReportUtils.parse_date(dbutils.widgets.get("data_interval_end"), 1) # D-1 = YESTERDAY
    today = AutoReportUtils.parse_date(dbutils.widgets.get("data_interval_end"), 0) # D-DAY
    
    # (!) TEST
#     data_interval_end = AutoReportUtils.parse_date("2023-03-02 14:00:00", 1)
except Exception as e:
    w_log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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
# MAGIC #### Abstract Class

# COMMAND ----------

class AutoReportAPI(ABC):
    """
    A class as parent class inherited by channels e.g. google
    """
    def __init__(self) -> None:
        pass
    
    @abstractmethod
    def proc_all(self):
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Utils
# MAGIC - Utils
# MAGIC   - Global
# MAGIC   - AutoReport
# MAGIC   - Google
# MAGIC - Response Valiator

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### PtbwaUtils

# COMMAND ----------

class PtbwaUtils:
    """
    A class for dealing with repeated functions globally
    """
    @staticmethod
    def check_result(domain, result):
        try:
            print(f"[{domain}-RESULT]")
            pprint.pprint(result)
            print()
            w_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### AutoReportUtils
# MAGIC - `2023-03-02`
# MAGIC   - V2 :: Add parse_date() 

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
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def convert_snake_to_camel(value: str) -> str:
        return ''.join(w.title() if i != 0 else w for i, w in enumerate(value.split('_')))
    
    @staticmethod
    def rename_columns(df: DataFrame) -> DataFrame:
        """
        A method for renaming columns of spark dataframe
        """
        try:
            for col in df.schema:
                renamed_col = AutoReportUtils.convert_snake_to_camel(col.name)
                df = df.withColumnRenamed(col.name, renamed_col)
            w_log.info(f"[SUCCESS-{sys._getframe().f_code.co_name.upper()}]NEW-COLS::{df.schema}")
            return df
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_json_rdd(json_rows: list) -> DataFrame:
        """
        A method for creating spark dataframe before saving to delta
        """
        try:
            json_rdd = sc.parallelize(json_rows)
            df = spark.read.json(json_rdd)
            return df
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{json_rows[0]}")
            raise e
    
    @staticmethod
    def create_path(*args):
        return os.path.join(*args)
    
    @staticmethod
    def save_to_delta(df: DataFrame, table_path: str, mode="append", **kwargs) -> None:
        """
        A method for saving spark dataframe to delta
        desc:
            table_path         e.g. {database}.{table_name}
        """
        try:
            (df.write.mode(mode).saveAsTable(table_path))
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{table_path}")
            raise e
    
    @staticmethod
    def delete_prev(table_path: str, date_col: str, start_date: str, end_date: str, **kwargs) -> None:
        """
        A method for deleting previous data to prevent duplication
        desc:
            table_path         e.g. {database}.{table_name}
            date_col           e.g. eventDate | dateStart | segmentDate (case-unchanged)
        """
        try:
            spark.sql(f"DELETE FROM {table_path} WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'")
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE::{table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### GoogleUtils
# MAGIC - Required
# MAGIC   - Set Service
# MAGIC   - Set Query
# MAGIC   - Set Search Request

# COMMAND ----------

class GoogleUtils:
    """
    A class for deling with repeated functions in google
    """ 
    _SERVICES = {
        "get_campaigns": "GoogleAdsService",
        "get_adgroups": "GoogleAdsService",
        "get_adgroup_ads": "GoogleAdsService",
        "get_keyword_views": "GoogleAdsService",
    }
    
    _QUERIES = {
        "get_campaigns": f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign_criterion.campaign,
                campaign_criterion.criterion_id,
                campaign_criterion.negative,
                campaign_criterion.type,
                campaign_criterion.keyword.text,
                campaign_criterion.keyword.match_type
            FROM
                campaign_criterion
            WHERE
                campaign.advertising_channel_type IN ('VIDEO', 'DISPLAY', 'DISCOVERY')
        """,
        "get_adgroups": f"""
            
        """,
        "get_adgroup_ads": f"""
            SELECT 
                segments.date,
                campaign.id, 
                campaign.name, 
                ad_group.id, 
                ad_group.name, 
                ad_group_ad.ad.name,
                ad_group_ad.ad.id,
                campaign.advertising_channel_sub_type,
                campaign.advertising_channel_type,
                ad_group_ad.ad.final_mobile_urls,
                ad_group_ad.ad.final_urls,
                metrics.view_through_conversions,
                metrics.conversions, 
                metrics.conversions_from_interactions_rate,
                metrics.cost_per_conversion, 
                metrics.all_conversions, 
                metrics.all_conversions_from_interactions_rate,
                metrics.cost_per_all_conversions, 
                metrics.cost_micros, 
                metrics.clicks, 
                metrics.interactions, 
                metrics.ctr, 
                metrics.impressions,
                metrics.video_quartile_p100_rate,
                metrics.video_quartile_p25_rate,
                metrics.video_quartile_p50_rate,
                metrics.video_quartile_p75_rate,
                metrics.video_views,
                metrics.video_view_rate
            FROM 
                ad_group_ad
        """,
        "get_keyword_views": f"""
            
        """,
    }
    
    _CLIENT_TYPES = {
        "get_campaigns": "SearchGoogleAdsStreamRequest",
        "get_adgroup_ads": "SearchGoogleAdsStreamRequest",
    }
    
    @staticmethod
    def create_filter_query(**kwargs):
        """
        A method for creating filter query based on rules e.g. campaign.id IN ({campaign_ids})
        kwargs:
            values: string e.g. {value} | 1,2,3
        """
        try:
            if "start_date" not in kwargs or "start_date" not in kwargs:
                raise ValueError("[NOT-FOUND-DATES(START/END)]REQUIRED")
            
            base_filter = f"WHERE segments.date BETWEEN '{kwargs['start_date']}' AND '{kwargs['end_date']}'"
            custom_filter = base_filter + f" AND {kwargs['resource']}.{kwargs['metric']} {kwargs['condition']} ('{kwargs['values']}')"
            return custom_filter
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_service(client: GoogleAdsClient, method: str):
        """
        A method for creating search request
        """
        try:
            ga_service = client.get_service(GoogleUtils._SERVICES[method])
            PtbwaUtils.check_result("SERVICE", ga_service.__dict__)
            return ga_service
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_search_request(client: GoogleAdsClient, method: str, customer_id: str, query: str) -> dict:
        """
        A method for creating search request
        """
        try:
            search_request = client.get_type(GoogleUtils._CLIENT_TYPES[method])
            search_request.customer_id = customer_id
            search_request.query = query
            PtbwaUtils.check_result("SEARCH-REQUEST", search_request.__dict__)
            return search_request
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_table_path(base_db: str, advertiser: str, channel: str) -> str:
        """
        A method for creating delta table path e.g. stats on each campaigns
        """
        base_path = f"{base_db}.{channel}_{advertiser}"
        return f"{base_path}_ad_stats"
    
    @staticmethod
    def convert_to_snake(col: str) -> str:
        return '_'.join(col.split())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Response Validator

# COMMAND ----------

class DatabricksAPIResponseValidator:
    
    _EXCLUDES = [
        "create_msg_format",
        "is_valid"
    ]
    
    """
    A class for validating API Response on codes e.g. 200, 400, 401, 500
    """
    @staticmethod
    def create_msg_format(func_name:str, status_code:int, error_code:str, message:str) -> str:
        return f"[FAIL-API-RESPONSE-VALIDATION({func_name}())]STATUS::{status_code}|CODE::{error_code}|MESSAGE::{message}"
    
    @staticmethod
    def is_valid(response):
        """
        A method for validating whole checklist based on below @staticmethod
        """
        attributions = DatabricksAPIResponseValidator.__dict__
        result = response.json()
        for key in attributions.keys():
            if key not in DatabricksAPIResponseValidator._EXCLUDES and isinstance(attributions[key], staticmethod):
                getattr(DatabricksAPIResponseValidator, key)(response)
    
    @staticmethod
    def is_ok(response):
        """
        A method for checking status code
        """
        if response.status_code != 200:
            raise Exception(
                DatabricksAPIResponseValidator.create_msg_format(
                    sys._getframe().f_code.co_name,
                    response.status_code,
                    result["error_code"],
                    result["message"]
                )
            )
    
    @staticmethod
    def has_error(response):
        """
        A method for validating error code, message
        """
        result = response.json()
        pass
        

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Decorator
# MAGIC - Validation

# COMMAND ----------

class DataValidator:
    
    _REQUIRED = {
        "_create": [
            "advertiser",
            "base_db",
            "channel",
            "base_path",
            "api_types",
            "customer_id",
            "config_path",
            "config"
        ]
    }
    
    _DO_VALIDATIONS = {
        "base_db": True,
        "advertiser": True,
        "channel": True,
        "base_path": True,
        "api_types": True,
        "config_path": True,
        "config": True,
        "customer_id": True,
        "api_version": False,
        "base_url": False,
        "target_paths": False,
    }

    def __init__(self, do_pre_validation: bool, do_post_validation: bool) -> None:
        self.do_pre_validation = do_pre_validation
        self.do_post_validation = do_post_validation

    def __call__(self, function):
        def inner(*args, **kwargs):
            method = function.__name__
            if self.do_pre_validation:
                DataValidator._pre_validation(args, method)

            result = function(*args, **kwargs)
            
            if self.do_post_validation:
                DataValidator._post_validation(result, method)
            
            PtbwaUtils.check_result("DATA-VALIDATION", result)
            return result
        return inner
    
    @staticmethod
    def _post_validation(result, method):
        """
        A method for validating after running method
        args:
            result: SeriesGoogleData
        default:
            1. required
            2. type
            3. (optional) value
        """
        if isinstance(result, SeriesGoogleData) and method in DataValidator._REQUIRED:
            # (!) required
            for required_key in DataValidator._REQUIRED[method]:
                if required_key not in result.__dict__:
                    raise ValueError(f"[NOT-FOUND-REQUIRED-KEY]{required_key}")
            
            # (!) type
            for k, v in result.__dict__.items():
                if k in RequestGoogleData.__dataclass_fields__ and not isinstance(v, RequestGoogleData.__dataclass_fields__[k].type):
                    raise ValueError(f"[TYPE-MISMATCH]KEY::{k}|TYPE::{RequestGoogleData.__dataclass_fields__[k].type}")
                
            # (!) (optional) value
            for k, v in result.__dict__.items():
                if DataValidator._DO_VALIDATIONS[k] and not getattr(DataValidator, f"is_valid_{k}")(v):
                    raise ValueError(f"[INVALID-VALUE]KEY::{k}|VALUE::{v}")
                    
    @staticmethod
    def is_valid_advertiser(advertiser: str):
        return advertiser in ["series", "kcar"]
    
    @staticmethod
    def is_valid_channel(channel: str):
        return channel in ["gad"]
    
    @staticmethod
    def is_valid_base_path(base_path: str):
        return base_path.startswith("/dbfs")
    
    @staticmethod
    def is_valid_api_types(api_types: list):
        for api_type in api_types:
            if api_type not in ["campaign", "adgroup", "adgroup_ad", "keyword_view", "adgroup_asset_view"]:
                return False
        return True
    
    @staticmethod
    def is_valid_base_db(base_db: str):
        candidate_dbs = ["auto_report", "ice", "cream", "default"]
        return base_db in candidate_dbs + [f"tt_{candidate_db}" for candidate_db in candidate_dbs]
    
    @staticmethod
    def is_valid_config_path(config_path: str):
        return (config_path.startswith("/dbfs/FileStore/configs/") and config_path.endswith(("json", "pickle")))
    
    @staticmethod
    def is_valid_config(config: str):
        """
        A method for validating required-configs to create google client
        """
        return (
            "developer_token" in config 
            and "refresh_token" in config
            and "client_id" in config
            and "client_secret" in config
            and "use_proto_plus" in config
        )
    
    @staticmethod
    def is_valid_customer_id(customer_id: str):
        return (len(customer_id) == 10 and customer_id.isnumeric()) # e.g. 123-456 => False
    
    @staticmethod
    def _pre_validation(result, method):
        """
        A method for validating after running method
        """
        pass

# COMMAND ----------

# RequestGoogleData.__dataclass_fields__["api_version"]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Request & Response
# MAGIC - @dataclass
# MAGIC   - Getter
# MAGIC   - Setter
# MAGIC - Required
# MAGIC   - Validation

# COMMAND ----------

@dataclass
class RequestCommonData:
    api_version: str
    base_url: str
    base_db: str
    advertiser: str
    channel: str

@dataclass
class RequestGoogleData(RequestCommonData):
    base_path: str
    api_types: list      # e.g. ["daily_exports"] => required sorting
    target_paths: dict   # e.g. {"daily_exports": "export"}
    customer_id: str     # e.g. {12345678910}

@dataclass
class ResponseCommonData:
    username: str
    password: str
    ip: str

@dataclass
class ResponseGoogleData(ResponseCommonData):
    username: str
    password: str
    ip: str

# COMMAND ----------

request_google_data = RequestGoogleData(
    advertiser="series",
    base_url="",
    base_db=database,
    api_version="",
    channel="gad",
    base_path="/dbfs/FileStore/configs/",
    api_types=[
        "campaign",
        "adgroup_ad",
#         "adgroup",
#         "keyword_view",
    ],
    target_paths={
        "campaign": "campaign",
        "adgroup_ad": "adgroup_ad",
#         "adgroup": "adgroup",
#         "keyword_view": "keyword_view",
    },
    customer_id="3269406390"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Data
# MAGIC - Source
# MAGIC   - `Request{Channel}Data`
# MAGIC - Naming
# MAGIC   - `{Advertiser}{Channel}Data`

# COMMAND ----------

class SeriesGoogleData:
    
    _CANDIDATES = [
        "advertiser",
        "base_url",
        "base_db",
        "api_version",
        "channel",
        "base_path",
        "api_types",
        "target_paths",
        "customer_id",
    ]
    
    def __init__(self, request_google_data:RequestGoogleData):
        data = {
            k: v for k, v in request_google_data.__dict__.items() if k in SeriesGoogleData._CANDIDATES
        }
        self.__dict__.update(data)
        PtbwaUtils.check_result("SERIES-GOOGLE-DATA(INIT)", self.__dict__)
    
    @DataValidator(do_pre_validation=False, do_post_validation=True)
    def _create(self):
        """
        A method creating additional data based on init
        validation:
            1. (optional) pre-conditions
            2. (optional) post-conditions
        """
        try:
            self.config_path = os.path.join(self.base_path, "google_ads_api_client.pickle")
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            
            return self
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

series_google_data = SeriesGoogleData(request_google_data)

# COMMAND ----------

series_google_data._create()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Validated Data

# COMMAND ----------

PtbwaUtils.check_result("VALIDATED-DATA", series_google_data.__dict__)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Save Credentials

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
# MAGIC #### Main API

# COMMAND ----------

class SeriesGoogleAPI(AutoReportAPI):
    """
    A class for processing google api procedures
    """
    
    def __init__(self, series_google_data:SeriesGoogleData) -> None:
        self.series_google_data = series_google_data
        self.client = GoogleAdsClient.load_from_dict(self.series_google_data.config)

        self.current = datetime.now() + timedelta(hours=9)
        
        # (!) daily + fail task
        self.yesterday = data_interval_end
        self.today = today
    
        # (!) previous
#         self.yesterday = "2023-02-24"
#         self.today = "2023-02-28"
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        default: 
            get_campaigns() - 1st => retrieve campaign_ids
        """
        self.series_google_data.api_types = list(set(self.series_google_data.api_types))
        if "campaign" in self.series_google_data.api_types:
            self.series_google_data.api_types.remove("campaign")
            self.series_google_data.api_types.insert(0, "campaign")
        
        for api_type in self.series_google_data.api_types:
            getattr(self, f"get_{api_type}s")(api_type)
            if api_type != "campaign":
                table_path = GoogleUtils.create_table_path(
                    base_db=self.series_google_data.base_db,
                    advertiser=self.series_google_data.advertiser,
                    channel=self.series_google_data.channel
                )
                PtbwaUtils.check_result("TABLE-PATH", table_path)
                
                AutoReportUtils.delete_prev(
                    table_path=table_path,
                    date_col="segmentsDate",
                    start_date=self.yesterday,
                    end_date=self.today,
                )
    
                AutoReportUtils.save_to_delta(
                    df=getattr(self, f"{api_type}_df"),
                    table_path=table_path,
                    mode="append"
                )
    
    def get_campaigns(self, api_type: str):
        """
        A method for getting {campaigns} e.g. id, name
        """
        try:
            method = sys._getframe().f_code.co_name

            query = GoogleUtils._QUERIES[method]
            PtbwaUtils.check_result("QUERY", query)
            
            ga_service = GoogleUtils.create_service(self.client, method)
            search_request = GoogleUtils.create_search_request(
                self.client,
                method,
                self.series_google_data.customer_id,
                query
            )
            
            stream = ga_service.search_stream(search_request)
            campaign_ids, campaign_names = [], []
            for batch in stream:
                for row in batch.results:
                    campaign_ids.append(row.campaign.id)
                    campaign_names.append(row.campaign.name)
            
            # (!) remove duplication
            self.campaign_ids = list(set(campaign_ids))
            PtbwaUtils.check_result("QUERY-EXECUTION", self.campaign_ids)
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_adgroup_ads(self, api_type: str):
        """
        A method for getting {adgroup ads} stats
        """
        try:
            method = sys._getframe().f_code.co_name
            
            query = GoogleUtils._QUERIES[method]
            filter_query = GoogleUtils.create_filter_query(
                start_date=self.yesterday,
                end_date=self.today,
                resource="campaign",
                metric="id",
                condition="IN",
                values="','".join([str(campaign_id) for campaign_id in series_google_api.campaign_ids]),
            )
            query += filter_query
            PtbwaUtils.check_result("QUERY", query)

            ga_service = GoogleUtils.create_service(self.client, method)
            search_request = GoogleUtils.create_search_request(
                self.client,
                method,
                self.series_google_data.customer_id,
                query
            )
            
            stream = ga_service.search_stream(search_request)
            json_rows = []
            for batch in stream:
                for row in batch.results:
                    segments = row.segments
                    campaign = row.campaign
                    adgroup = row.ad_group
                    ad = MessageToDict(row.ad_group_ad.ad) # e.g. repeated-fields, array-like, object
                    metrics = row.metrics
                    
                    json_rows.append(json.dumps({
                        "networkType": segments.ad_network_type,
                        "segmentsDate": segments.date,
                        "campaignId": campaign.id,
                        "campaignName": campaign.name,
                        "campaignAdvertisingChannelType": campaign.advertising_channel_type,
                        "campaignAdvertisingChannelSubType": campaign.advertising_channel_sub_type,
                        "adgroupId": adgroup.id,
                        "adgroupName": adgroup.name,
                        "adId": ad["id"] if "id" in ad else "",
                        "adName": ad["name"] if "name" in ad else "",
                        "adFinalUrls": ad["finalUrls"],
                        "metricsImpressions": metrics.impressions,
                        "metricsCostMicros": metrics.cost_micros,
                        "metricsClicks": metrics.clicks,
                        "metricsInteractions": metrics.interactions,
                        "metricsCtr": metrics.ctr,
                        "metricsViewThroughConversions": metrics.view_through_conversions,
                        "metricsConversions": metrics.conversions,
                        "metricsConversionsFromInteractionRate": metrics.conversions_from_interactions_rate,
                        "metricsCostPerConversions": metrics.cost_per_conversion, 
                        "metricsAllConversions": metrics.all_conversions, 
                        "metricsAllConversionsFromInteractionRate": metrics.all_conversions_from_interactions_rate,
                        "metricsCostPerAllConversions": metrics.cost_per_all_conversions, 
                        "metricsVideoQuartileP100Rate": metrics.video_quartile_p100_rate,
                        "metricsVideoQuartileP25Rate": metrics.video_quartile_p25_rate,
                        "metricsVideoQuartileP50Rate": metrics.video_quartile_p50_rate,
                        "metricsVideoQuartileP75Rate": metrics.video_quartile_p75_rate,
                        "metricsVideoViews": metrics.video_views,
                        "metricsVideoViewRate": metrics.video_view_rate,
                        "customerName": "SERIES" if self.series_google_data.customer_id == "3269406390" else "SERIES Re-engagement"
                    }))
    
            PtbwaUtils.check_result("QUERY-EXECUTION", json_rows[0])
            setattr(self, f"{api_type}_df", AutoReportUtils.create_json_rdd(json_rows))
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

series_google_api = SeriesGoogleAPI(series_google_data)

# COMMAND ----------

PtbwaUtils.check_result("SERIES-GOOGLE-API-ATTRIBUTIONS", series_google_api.__dict__)

# COMMAND ----------

PtbwaUtils.check_result("SERIES-GOOGLE-DATA", series_google_api.__dict__["series_google_data"].__dict__)

# COMMAND ----------

try:
    series_google_api.proc_all()
except Exception as e:
    w_log.error("[FAIL-SERIES-GOOGLE]")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save to Delta Manually

# COMMAND ----------

# AutoReportUtils.save_to_delta(
#     df=getattr(series_google_api, f"adgroup_ad_df"),
#     table_path="tt_auto_report.gad_series_ad_stats",
#     mode="append"
# )

# COMMAND ----------

# series_google_api.adgroup_ad_df.printSchema()

# COMMAND ----------

# spark.conf.get("spark.databricks.delta.autoCompact.minNumFiles")