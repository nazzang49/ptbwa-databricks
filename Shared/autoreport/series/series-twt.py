# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Twitter Ads API
# MAGIC - wiki
# MAGIC   - https://ptbwa.atlassian.net/wiki/spaces/PTBWA/pages/13598721/auto-report+twitter+ads+api
# MAGIC - main domains
# MAGIC   - promoted_tweets (ptbwa side)
# MAGIC   - organic_tweets (client side)
# MAGIC   - tweets
# MAGIC   - stats

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Logging

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
t_log = log4jLogger.LogManager.getLogger("TWITTER-ADS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load Packages

# COMMAND ----------

!python -m pip install twitter-ads
!python -m pip install AttrDict

# COMMAND ----------

import pickle
import os
import json
import pprint
import time
import pandas as pd
import sys

from urllib.parse import urlparse, parse_qs
from ptbwa_utils import DailyIterable

from pyspark.sql.types import *
from pyspark.sql.functions import *

from datetime import datetime, timedelta
from attrdict import AttrDict
from enum import Enum, unique
from abc import ABC, abstractmethod
from typing import *
from collections import defaultdict

from twitter_ads.http import Request
from twitter_ads.client import Client
from twitter_ads.campaign import Campaign, LineItem, TargetingCriteria
from twitter_ads.enum import ENTITY_STATUS, OBJECTIVE, PLACEMENT, PRODUCT, METRIC_GROUP, GOAL, GRANULARITY
from twitter_ads.utils import split_list, FlattenParams

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
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
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
    t_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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
    t_log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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
# MAGIC ### Class Definition

# COMMAND ----------

class AutoReportAPI(ABC):
    """
    A class as parent class inherited by channels e.g. twitter-ads
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
    STATS = "stats"
    ENTITY = "entity"
    TWEETS = "tweets"
    CAMPAIGNS = "campaigns"
    LINE_ITEMS = "line_items"
    PROMOTED_TWEETS = "promoted_tweets"
    
    GET = "get"
    POST = "post"
    
class FieldGenerator(Enum):
    """
    A enum class for generating field parameter
    """
    pass
    
class ParamGenerator(Enum):
    """
    A enum class for generating param parameter
    """
#     STATS = f"entity=ORGANIC_TWEET&start_time=2022-06-01&end_time=2022-06-07&granularity=DAY&placement=ALL_ON_TWITTER&metric_groups=ENGAGEMENT,VIDEO,BILLING"
    STATS = f"entity=PROMOTED_TWEET&start_time=2022-06-08&end_time=2022-06-15&granularity=DAY&placement=ALL_ON_TWITTER&metric_groups=ENGAGEMENT,BILLING,VIDEO"
    
#     TWEETS = f"tweet_type=PUBLISHED&timeline_type=ORGANIC"
    TWEETS = f"tweet_type=PUBLISHED"
    CAMPAIGNS = f""
    LINE_ITEMS = f""
    PROMOTED_TWEETS = f"sort_by=created_at-desc&count=900&with_deleted=true"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### PTBWA Utils Module
# MAGIC - create common params
# MAGIC - create common logic

# COMMAND ----------

class PtbwaUtils:
    """
    A class for dealing with repeated functions
    """
    
    _REQUEST_URL_MAP = {
        "google": {
            
        },
        "facebook": {
            
        },
        "twitter" : {
            "stats": ""
        }
    }
    
    _REQUEST_PARAMS_MAP = {
        "google": {
            
        },
        "facebook": {
            
        },
        "twitter" : {
            "stats": ""
        }
    }
    
    @staticmethod
    def get_request_url(channel_type, domain, id=None):
        """
        A method for mapping request url
        args:
            channel_type: e.g. google, facebook, twitter
        """
        pass
    
    @staticmethod
    def get_request_params(channel_type, domain, id=None):
        """
        A method for mapping request params
        args:
            channel_type: e.g. google, facebook, twitter
        """
        pass
    
    @staticmethod
    def logging():
        """
        A method for custom logging
        """
        pass
    
    @staticmethod
    def check_result(domain, result):
        try:
            print(f"[{domain}-RESULT]")
            pprint.pprint(result)
            print()
            t_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

class TwitterAdsApi(AutoReportAPI):
  
    _API_VERSION = '11'
    
    _DOMAINS = [
        "entity",
        "tweets",
        "stats",
        "promoted_tweets",
        "line_items",
        "campaigns"
    ]
    
    _SUB_DOMAINS = [
        "entity",
        "tweets",
        "stats",
        "promoted_tweets",
        "organic_tweets",
    ]
    
    # define manually
    _TWEETS_DROP_COLS = ["id", "createdAt", "userTranslatorType", "userWithheldInCountries", "userFollowersCount", "entitiesHashtags", "entitiesSymbols", "entitiesUserMentions", "entitiesUrls"]
    _STATS_DROP_COLS = []
    _PROMOTED_TWEETS_DROP_COLS = []

    def __init__(self, customer_id, params=None) -> None:
        self.customer_id = customer_id
#         self.today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
        self.yesterday = data_interval_end
        
        self.base_path = "/dbfs/FileStore/configs/"
        self.config_name = "twitter_ads_api_client.pickle"
        
        self.tweets = []
        self.stats = []
        self.line_items = []
        self.campaigns = []
        self.channel = "twt"
        
        self.base_db = database
        self.advertiser = "series"
        self.table_path = self.create_table_path()
        self.params = params
        
        # todo validation of properties
#         WidgetValidator.is_valid_property(self.args)

    def proc_all(self):
        """
        A method for processing all methods orderly
        """
        self._set_config()
        self._set_client()
        self.get_promoted_tweets()
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
            t_log.error(f"[FAIL-IS-CONFIG-EXIST]{self.config_path}")
            raise e
    
    def _set_config(self):
        """
        A method for setting config as dict-based credentials
        """
        try:
            self._is_config_exist()
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            t_log.info(f"[CHECK-CONFIG]{self.config}")
        except Exception as e:
            t_log.error("[FAIL-SET-CONFIG]")
            raise e
    
    def _set_client(self):
        """
        A mehtod for setting twitter ads api client from credentials
        """
        try:
            self.client = Client(self.config["consumer_key"], 
                                 self.config["consumer_secret"], 
                                 self.config["access_token"], 
                                 self.config["access_token_secret"])
            PtbwaUtils.check_result("CLIENT", self.client)
        except Exception as e:
            t_log.error(f"[FAIL-SET-CLIENT]{self.config}")
            raise e
    
    def post_process_promoted_tweets(self, domain, result):
        """
        A method for post processing after calling get_promoted_tweets()
        """
        try:
            self.parse_result(domain)
            
            # get stats and tweets
            for k, ids in self.parsed_result.items():
                sub_domain = k.split("_")[0] + "s"
                if sub_domain in TwitterAdsApi._SUB_DOMAINS:
                    for i in range(1, len(ids) // 20):
                        time.sleep(0.5)
                        target_ids = ','.join(ids[20 * (i - 1):20 * i]) # max 20 per 1-request
                        getattr(self, f"get_{sub_domain}")(target_ids)
                    
                    # transformation on stats and tweets
                    getattr(self, f"convert_{sub_domain}_to_df")()
                    getattr(self, "convert_from_pandas_to_spark")(sub_domain)
        except Exception as e:
            t_log.error(f"[FAIL-POST-PROCESS-PROMOTED-TWEETS]")
            raise e
    
    def get_promoted_tweets(self):
        """
        A method for getting promoted tweets by {account(=customer) id}
        """
        try:
            domain = "PROMOTED_TWEETS"
            params = self.get_params_str(domain)
            request_params = self.create_request_params(domain.lower(), params)
            request_url = self.create_request_url(domain.lower())
            
            response = Request(self.client, ArgumentGenerator.GET.value, request_url, params=request_params).perform()
            PtbwaUtils.check_result(domain, response.body)
            self.promoted_tweets_df = pd.json_normalize(response.body, record_path=['data'])
            
            # (!) get stats and tweets
            self.post_process_promoted_tweets(domain, response.body)
            
            # (!) trans on promoted_tweets
            self.promoted_tweets_df.rename(columns = {col: self.rename_column(col) for col in self.promoted_tweets_df.columns}, inplace = True)
            
            self.promoted_tmp_df = self.promoted_tweets_df.copy()
            
            self.promoted_tweets_df = self.promoted_tweets_df[["id", "lineItemId", "tweetId"]]
            self.promoted_tweets_df["statId"] = self.promoted_tweets_df["id"]
            self.promoted_tweets_df.drop(columns=["id"], inplace=True)
            self.convert_from_pandas_to_spark(domain.lower())
    
            # (!) first join
            self.proc_join()
            self.cast_column_type()
            
            # (!) get line items
            target_ids = ','.join(self.parsed_result["line_item_ids"])
            self.get_line_items(target_ids)
            self.convert_line_items_to_df()
            self.convert_from_pandas_to_spark("line_items")
            
            # (!) get campaigns
            target_ids = ','.join(self.line_items_df["campaignId"].tolist())
            self.get_campaigns(target_ids)
            self.convert_campaigns_to_df()
            self.convert_from_pandas_to_spark("campaigns")
            
            # (!) final join
            self.final_join()
        except Exception as e:
            t_log.error(f"[FAIL-GET-PROMOTED-TWEETS]")
            raise e
    
#     @FlattenParams
    def get_tweets(self, tweet_id):
        """
        A method for getting tweets by {account(=customer) id}
        """
        try:
            assert tweet_id is not None, "[NOT-FOUND-TWEET-ID]REQUIRED"
            
            domain = "TWEETS"
            params = self.get_params_str(domain)
            request_params = self.create_request_params(domain.lower(), params, tweet_ids=tweet_id)
            request_url = self.create_request_url(domain.lower())
            
            response = Request(self.client, ArgumentGenerator.GET.value, request_url, params=request_params).perform()
            self.tweets += response.body["data"]
            
#             PtbwaUtils.check_result(domain, response.body)
        except Exception as e:
            t_log.error(f"[FAIL-GET-TWEETS]")
            raise e
            
#     @FlattenParams
    def get_campaigns(self, campaign_id):
        """
        A method getting line items (called after 3-main domains)
        """
        try:
            assert campaign_id is not None, "[NOT-FOUND-CAMPAIGNS-ID]REQUIRED"

            domain = "CAMPAIGNS"
            params = self.get_params_str(domain)
            request_params = self.create_request_params(domain.lower(), params, campaign_ids=campaign_id)
            request_url = self.create_request_url(domain.lower())
            
            response = Request(self.client, ArgumentGenerator.GET.value, request_url, params=request_params).perform()
            self.campaigns += response.body["data"]
            
#             PtbwaUtils.check_result(domain, response.body)
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{campaign_id}")
            raise e        
            
#     @FlattenParams
    def get_line_items(self, line_item_id):
        """
        A method getting line items (called after 3-main domains)
        """
        try:
            assert line_item_id is not None, "[NOT-FOUND-LINE-ITEMS-ID]REQUIRED"

            domain = "LINE_ITEMS"
            params = self.get_params_str(domain)
            request_params = self.create_request_params(domain.lower(), params, line_item_ids=line_item_id)
            request_url = self.create_request_url(domain.lower())
            
            response = Request(self.client, ArgumentGenerator.GET.value, request_url, params=request_params).perform()
            self.line_items += response.body["data"]
            
#             PtbwaUtils.check_result(domain, response.body)
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{line_item_id}")
            raise e
    
#     @FlattenParams
    def get_stats(self, stat_id):
        """
        A method for getting stats by {account(=customer) id}
        """
        try:
            assert stat_id is not None, "[NOT-FOUND-STAT-ID]REQUIRED"
            
            domain = "STATS"
            params = self.get_params_str(domain)
            request_params = self.create_request_params(domain.lower(), params, entity_ids=stat_id)
            request_url = self.create_request_url(domain.lower())
            
            # (!) inserted params e.g. load previous data
            if self.params:
                self.params["entity_ids"] = request_params["entity_ids"]
            
            # (!) check {self.params} after adding data_interval_end
            PtbwaUtils.check_result("JUST-BEFORE-STATS", self.params)
            
            response = Request(self.client, 
                               ArgumentGenerator.GET.value, 
                               request_url, 
                               params=self.params if self.params else request_params).perform()
            self.stats += response.body["data"]
            
            PtbwaUtils.check_result(domain, response.body)
        except Exception as e:
            t_log.error(f"[FAIL-GET-ENTITIES]")
            raise e
    
    def convert_snake_to_camel(self, value):
        """
        A method for converting snake to camel
        """
        return ''.join(w.title() if i != 0 else w for i, w in enumerate(value.split('_')))
    
    def rename_column(self, col):
        """
        A method for renaming column from pandas df
        """
        # todo change below line
#         self.promoted_tweets_df.rename(columns = {col: self.rename_column(col) for col in self.promoted_tweets_df.columns}, inplace = True)

        return self.convert_snake_to_camel(col.replace(".", "_"))
    
    def proc_join(self):
        """
        A method for joining whole spark df firstly
        """
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.join(self.spark_tweets_df, on=["tweetId"], how="inner")
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.join(self.spark_stats_df, on=["statId"], how="inner")
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.distinct()
        
    def final_join(self):
        """
        A method for joining whole spark df finally
        """
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.join(self.spark_line_items_df, on=["lineItemId"], how="inner")
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.join(self.spark_campaigns_df, on=["campaignId"], how="inner")
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.dropDuplicates([col for col in self.spark_promoted_tweets_df.columns if "metrics" in col])
        self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.drop("level_0", "level_1")
    
    def drop_unused_columns(self, domain):
        """
        A method for dropping unused or null nested columns
        """
        getattr(self, f"{domain}_df").drop(columns=getattr(TwitterAdsApi, f"_{domain.upper()}_DROP_COLS"), inplace=True)
    
    def cast_column_type(self):
        """
        A method for changing column type based on final df e.g. nulltype to stringtype
        """
        for s in self.spark_promoted_tweets_df.schema:
            if isinstance(s.dataType, NullType):
                self.spark_promoted_tweets_df = self.spark_promoted_tweets_df.withColumn(s.name, col(s.name).cast("string"))
    
    def convert_campaigns_to_df(self):
        """
        A method for converting campaigns data to pandas df
        """
        try:
            self.campaigns_df = pd.json_normalize(self.campaigns)
            self.campaigns_df = self.campaigns_df[["name", "id"]]
            self.campaigns_df.rename(columns={"id": "campaignId", "name": "campaignName"}, inplace=True)
        except Exception as e:
            t_log.error(f"[FAIL-CONVERT-CAMPAIGNS-TO-DF]")
            raise e
    
    def convert_line_items_to_df(self):
        """
        A method for converting line items data to pandas df
        """
        try:
            self.line_items_df = pd.json_normalize(self.line_items)
            self.line_items_df.rename(columns = {col: self.rename_column(col) for col in self.line_items_df.columns}, inplace = True)
            self.line_items_df = self.line_items_df[["name", "id", "campaignId"]]
            self.line_items_df.rename(columns={"id": "lineItemId", "name": "lineItemName"}, inplace=True)
        except Exception as e:
            t_log.error(f"[FAIL-CONVERT-LINE-ITEMS-TO-DF]")
            raise e
    
    def convert_tweets_to_df(self):
        """
        A method for converting tweets data to pandas df
        """
        try:
            self.tweets_df = pd.json_normalize(self.tweets)
            self.tweets_df.rename(columns = {col: self.rename_column(col) for col in self.tweets_df.columns}, inplace = True)
            self.tweets_df = self.tweets_df[["name", "tweetId"]]
        except Exception as e:
            t_log.error(f"[FAIL-CONVERT-TWEETS-TO-DF]")
            raise e
    
    def convert_stats_to_df(self):
        """
        A method for converting stats data to pandas df
        """
        try:
            self.stats_df = pd.json_normalize(
                self.stats, 
                record_path =['id_data'],
                meta=["id"]
            )
            
            cols = self.stats_df.columns
            params = parse_qs(getattr(ParamGenerator, "STATS").value)
            start_time = datetime.strptime(self.params["start_time"], "%Y-%m-%d") if self.params else datetime.strptime(params["start_time"][0], "%Y-%m-%d")
            end_time = datetime.strptime(self.params["end_time"], "%Y-%m-%d") if self.params else datetime.strptime(params["end_time"][0], "%Y-%m-%d")
            diff_days = (end_time - start_time).days
            
            print(f"START_TIME::{start_time}")
            print(f"END_TIME::{end_time}")
            print(f"DIFF_DAYS::{diff_days}")
            
            rows = []
            for col in cols:
                rows.append(self.stats_df[col].apply(pd.Series).stack())
            self.stats_df = pd.concat(rows, axis=1, keys=cols)
        
            # (!) drop NaN
            self.stats_df.dropna(subset=[col for col in cols if 'metrics' in col], how="all", inplace=True)
            
            # (!) comparison df
            self.tmp_df = self.stats_df.copy()
            
            # (!) add date
            self.stats_df["segmentDate"] = [day for day in DailyIterable(start_time, end_time)] * (len(self.stats_df) // diff_days)
            
            # (!) id replication
            self.stats_df.reset_index(inplace=True)
            self.stats_df["id"] = [self.stats_df.iloc[(i // diff_days) * diff_days]["id"] for i in self.stats_df.index]
            self.stats_df["statId"] = self.stats_df["id"]
            
            self.stats_df = self.stats_df.fillna(0.0) 
            self.stats_df.rename(columns = {col: self.rename_column(col) for col in cols}, inplace = True)
        except Exception as e:
            t_log.error(f"[FAIL-CONVERT-STATS-TO-DF]")
            raise e
    
    def parse_result(self, domain):
        """
        A method for parsing result based on domain
            args:
                id for promoted_tweets
                tweet_id for organic_tweets
        """
        try:
            self.parsed_result = defaultdict(list)
            self.parsed_result["stat_ids"] = self.promoted_tweets_df["id"].tolist()
            self.parsed_result["tweet_ids"] = self.promoted_tweets_df["tweet_id"].tolist()
            self.parsed_result["line_item_ids"] = self.promoted_tweets_df["line_item_id"].drop_duplicates().tolist()
        except Exception as e:
            t_log.error(f"[FAIL-PARSE-RESULT]{domain}")
            raise e
    
    def create_request_params(self, domain, params, **kwargs):
        """
        A method creating final request params based on domain e.g. tweets, entities
        """
        try:
            request_params = dict()
            if params:
                for param in params.split("&"):
                    tmp = param.split("=")
                    request_params[tmp[0]] = tmp[1]
            
            for k, v in kwargs.items():
                request_params[k] = v
                
            PtbwaUtils.check_result("REQUEST-PARAMS", request_params)
            return request_params
        except Exception as e:
            t_log.error(f"[FAIL-CREATE-REQUEST-PARAMS]{domain}/{params}/{kwargs}")
            raise e
    
    def get_params_str(self, domain):
        """
        A method for creating params string as response data
        """
        try:
            return getattr(ParamGenerator, domain).value
        except Exception as e:
            t_log.error(f"[FAIL-CREATE-PARAMS-STR]{domain}")
            raise e
    
    def create_request_url(self, domain):
        """
        A method for creating request url
            ref. twitter ads api docs
            e.g. domain = stats, tweets, promoted_tweets, etc.
        """
        try:
            assert domain in TwitterAdsApi._DOMAINS, f"[NOT-FOUND-DOMAINS]{domain}"
            base_url = f'/{TwitterAdsApi._API_VERSION}'
            
            if domain == ArgumentGenerator.TWEETS.value:
                request_url = f'{base_url}/accounts/{self.customer_id}/{domain}'
            elif domain == ArgumentGenerator.STATS.value:
                request_url = f'{base_url}/{domain}/accounts/{self.customer_id}'
            elif domain == ArgumentGenerator.PROMOTED_TWEETS.value:
                request_url = f'{base_url}/accounts/{self.customer_id}/{domain}'
            elif domain == ArgumentGenerator.LINE_ITEMS.value:
                request_url = f'{base_url}/accounts/{self.customer_id}/{domain}'
            elif domain == ArgumentGenerator.CAMPAIGNS.value:
                request_url = f'{base_url}/accounts/{self.customer_id}/{domain}'
            else:
                raise ValueError(f"[NOT-FOUND-DOMAINS]{domain}")
            PtbwaUtils.check_result("REQUEST-URL", request_url)
            return request_url
        except Exception as e:
            t_log.error(f"[FAIL-CREATE-REQUEST-URL]{domain}")
            raise e
    
    def convert_from_pandas_to_spark(self, domain):
        """
        A method for converting from pandas to spark
        """
        try:
            setattr(self, f"spark_{domain}_df", spark.createDataFrame(getattr(self, f"{domain}_df")))
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{domain}")
            raise e
    
    def create_table_path(self):
        """
        A method for creating saving point path
        """
        table_path = f"{self.base_db}.{self.channel}_{self.advertiser}_ad_stats"
        PtbwaUtils.check_result("TABLE-PATH", table_path)
        return table_path
    
    def save_to_delta(self):
        """
        A method for saving stat data to delta
        """
        try:
            (self.spark_promoted_tweets_df
                 .write
                 .mode("append")
                 .saveAsTable(self.table_path)
            )
        except Exception as e:
            t_log.error(f"[FAIL-SAVE-TO-DELTA]{self.table_path}")
            raise e
            
    def delete_prev(self):
        """
        A method deleting previous twitter ads naver series keyword stats
        """
        try:
            spark.sql(f"delete from {self.table_path} where segmentDate = '{self.yesterday}'")
        except Exception as e:
            t_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{self.table_path}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Processing
# MAGIC - (Default) Naver Series

# COMMAND ----------

# NAVER SERIES
s = data_interval_end
e = today

print(s)
print(e)

params = f"entity=PROMOTED_TWEET&start_time={s}&end_time={e}&granularity=DAY&placement=ALL_ON_TWITTER&metric_groups=ENGAGEMENT,BILLING,VIDEO"
request_params = dict()
for param in params.split("&"):
    tmp = param.split("=")
    request_params[tmp[0]] = tmp[1]

twitter_ads_api = TwitterAdsApi('18ce5563nt8', request_params)

# COMMAND ----------

try:
    twitter_ads_api.proc_all()
except Exception as e:
    if "can not infer schema from empty dataset" == e.args[0]:
        t_log.info("[SUCCESS-SERIES-TWT]NO-RESULT")
    else:
        t_log.error("[FAIL-SERIES-TWT]")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Comparison DF
# MAGIC - validation on ID

# COMMAND ----------

# twitter_ads_api.spark_tweets_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load Previous Data Daily
# MAGIC - (optional) organic_tweets
# MAGIC - promoted_tweets
# MAGIC - stats
# MAGIC - tweets

# COMMAND ----------

# start_date = datetime.strptime("2022-06-02", "%Y-%m-%d")
# end_date = datetime.strptime("2022-06-09", "%Y-%m-%d")

# for s in DailyIterable(start_date, end_date):
#     try:
#         e = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
#         params = f"entity=PROMOTED_TWEET&start_time={s}&end_time={e}&granularity=DAY&placement=ALL_ON_TWITTER&metric_groups=ENGAGEMENT,BILLING,VIDEO"

#         request_params = dict()
#         for param in params.split("&"):
#             tmp = param.split("=")
#             request_params[tmp[0]] = tmp[1]

#         twitter_ads_api = TwitterAdsApi('18ce5563nt8', request_params)
#         twitter_ads_api.proc_all()
#         time.sleep(1)
#     except Exception as e:
#         t_log.error(f"[LOAD-PREVIOUS-DATA-DAY]{s}")
#         t_log.error(f"[LOAD-PREVIOUS-DATA]{e}")
#         continue