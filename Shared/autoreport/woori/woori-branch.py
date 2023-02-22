# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### WooriBank Branch API
# MAGIC - Wiki
# MAGIC   - Branch
# MAGIC     - https://ptbwa.atlassian.net/wiki/spaces/DS/pages/82313248/AutoReport+Branch+API
# MAGIC   - Architecture
# MAGIC     - https://ptbwa.atlassian.net/wiki/spaces/DS/pages/84049921/AutoReport+Architecture+V2
# MAGIC - API Docs
# MAGIC   - https://help.branch.io/developers-hub/reference/apis-overview
# MAGIC - `2023-01-10` V1 | Init | @jinyoung.park

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
w_log = log4jLogger.LogManager.getLogger("WOORI-BRANCH")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install AttrDict
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

# try:
#     # (!) LIVE
#     env = dbutils.widgets.get("env")
    
#     # (!) TEST
# #     env = "dev"
# except Exception as e:
#     w_log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

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
    A class as parent class inherited by channels e.g. branch
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
# MAGIC - Enum
# MAGIC - Global Util
# MAGIC - Response Valiator

# COMMAND ----------

# class ArgumentGenerator(Enum):
#     """
#     A enum class for validating arguments and generating common types
#     """
#     pass
    
# class FieldGenerator(Enum):
#     """
#     A enum class for generating field parameter
#     """
#     pass
    
# class ParamGenerator(Enum):
#     """
#     A enum class for generating param parameter
#     """
#     pass

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
            
            
    # (!) Additional

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### AutoReportUtils

# COMMAND ----------

class AutoReportUtils:
    """
    A class for dealing with repeated functions in autoreport
    """
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
    def save_to_delta(df: DataFrame, table: str, mode="append", **kwargs) -> None:
        """
        A method for saving spark dataframe to delta
        desc:
            table         e.g. {database}.{table_name}
        """
        try:
            (df.write.mode(mode).saveAsTable(table))
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]{table}")
            raise e
    
    @staticmethod
    def delete_prev(table: str, date_col: str, criteria_date: str, **kwargs) -> None:
        """
        A method for deleting previous data to prevent duplication
        desc:
            table         e.g. {database}.{table_name}
            date_col      e.g. eventDate | dateStart | segmentDate (case-unchanged)
            criteria_date e.g. 2022-10-01
        """
        try:
            spark.sql(f"DELETE FROM {table} WHERE {date_col} = '{criteria_date}'")
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE::{table_path}")
            raise e
    
    @staticmethod
    def get(**kwargs) -> None:
        """
        A method for calling {get} api
        """
        try:
            response = requests.post(
                url=self.woori_branch_data.request_urls[kwargs["target_path"]],
                headers=BranchUtils.create_request_headers(kwargs["method"]),
                json=BranchUtils.create_request_params(method)
            )
            
            if not DatabricksAPIResponseValidator.is_valid(response):
                raise ValueError("[INVALID-RESPONSE]")
            
            return response
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def post(**kwargs) -> None:
        """
        A method for calling {post} api
        """
        try:
            response = requests.post(
                url=self.woori_branch_data.request_urls[target_path],
                headers=BranchUtils.create_request_headers(method),
                json=BranchUtils.create_request_params(method)
            )
        
            if not DatabricksAPIResponseValidator.is_valid(response):
                raise ValueError("[INVALID-RESPONSE]")
                
            return response
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### BranchUtils

# COMMAND ----------

class BranchUtils:
    """
    A class for deling with repeated functions in branch
    """ 
    
    _REQUEST_HEADERS = {
        "post_export": {
            "accept": "application/json",
            "content-type": "application/json"
        },
        "get_export": {
            "accept": "application/json"
        },
    }
    
    _REQUEST_PARAMS = {
         "post_export": {
#              "export_date": "2022-07-15"
         }
    }
    
    @staticmethod
    def create_request_headers(method: str) -> dict:
        """
        A method for creating request headers based on {method} name e.g. post_export
        """
        try:
            request_headers = BranchUtils._REQUEST_HEADERS[method]
            PtbwaUtils.check_result("REQUEST-HEADERS", request_headers)
            return request_headers
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_request_params(method: str) -> dict:
        """
        A method for creating request params based on {method} name e.g. post_export
        """
        try:
            request_params = BranchUtils._REQUEST_PARAMS[method]
            PtbwaUtils.check_result("REQUEST-PARAMS", request_params)
            return request_params
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_table_path(base_db, advertiser, channel) -> str:
        """
        A method for creating delta table path e.g. stats
        """
        base_path = f"{base_db}.{channel}_{advertiser}"
        return f"{base_path}_mmp_raw" if "log" in domain else f"{base_path}_mmp_stats"
    
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
        
        
        # ======================== (!) TODO
        
    # (!) Additional

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
            "base_url",
            "base_db",
            "api_version",
            "channel",
            "base_path",
            "api_types",
            "target_paths",
        ]
    }
    
    _DO_VALIDATIONS = {
        "request_urls": True,
        "api_version": True,
        "base_url": False,
        "base_db": True,
        "advertiser": True,
        "channel": True,
        "base_path": True,
        "api_types": True,
        "target_paths": True,
        "fields_path": True,
        "config_path": True,
#         "config": True,
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
            result: WooriBranchData
        default:
            1. required
            2. type
            3. (optional) value
        """
        if isinstance(result, WooriBranchData) and method in DataValidator._REQUIRED:
            # (!) required
            for required_key in DataValidator._REQUIRED[method]:
                if required_key not in result.__dict__:
                    raise ValueError(f"[NOT-FOUND-REQUIRED-KEY]{required_key}")
            
            # (!) type
            for k, v in result.__dict__.items():
                if k in RequestBranchData.__dataclass_fields__ and not isinstance(v, RequestBranchData.__dataclass_fields__[k].type):
                    raise ValueError(f"[TYPE-MISMATCH]KEY::{k}|TYPE::{RequestBranchData.__dataclass_fields__[k].type}")
                
            # (!) (optional) value
            for k, v in result.__dict__.items():
                if DataValidator._DO_VALIDATIONS[k] and not getattr(DataValidator, f"is_valid_{k}")(v):
                    raise ValueError(f"[INVALID-VALUE]KEY::{k}|VALUE::{v}")
            
            # (!) (additional) value
            if not DataValidator.is_valid_pair(result.api_types, result.target_paths, result.request_urls):
                raise ValueError(f"[INVALID-PAIR]PAIR::[target_paths, request_urls]")
                    
    @staticmethod
    def is_valid_request_urls(request_urls: dict):
        for request_url in request_urls.values():
            if not validators.url(request_url):
                return False
        return True
    
    @staticmethod
    def is_valid_base_db(base_db: str):
        candidate_dbs = ["auto_report", "ice", "cream", "default"]
        return base_db in candidate_dbs + [f"tt_{candidate_db}" for candidate_db in candidate_dbs]
    
    @staticmethod
    def is_valid_advertiser(advertiser: str):
        return advertiser in ["woori", "webtoon-us"]
    
    @staticmethod
    def is_valid_channel(channel: str):
        return channel in ["branch"]
    
    @staticmethod
    def is_valid_base_path(base_path: str):
        return base_path.startswith("/dbfs")
    
    @staticmethod
    def is_valid_api_version(api_version: str):
        return re.compile("[v|V][0-9]+").match(api_version)
    
    @staticmethod
    def is_valid_api_types(api_types: list):
        for api_type in api_types:
            if api_type not in ["daily_exports", "cohort"]:
                return False
        return True
    
    @staticmethod
    def is_valid_target_paths(target_paths: dict):
        for target_path in target_paths.values():
            if target_path not in ["export", "cohort"]:
                return False
        return True
    
    @staticmethod
    def is_valid_fields_path(fields_path: str):
        print(fields_path)
        return (fields_path.startswith("/dbfs/FileStore/configs/") and fields_path.endswith(("csv", "json", "pickle")))
    
    @staticmethod
    def is_valid_config_path(config_path: str):
        return (config_path.startswith("/dbfs/FileStore/configs/") and config_path.endswith(("json", "pickle")))
    
    @staticmethod
    def is_valid_config(config: dict):
        if isinstance(config, dict):
            return False
        
        for key in config.keys():
            if key not in ["branch_key", "branch_secret", "app_id", "organization_id", "access_token"]:
                return False
        return True
    
    @staticmethod
    def is_valid_pair(api_types: list, request_urls: dict, target_paths: dict):
        """
        A method for validating api_types(1) : target_paths(1) : request_urls(1)
        """
        if len(api_types) != len(request_urls) and len(request_urls) != len(target_paths):
            return False
        
        for api_type in api_types:
            if api_type not in request_urls:
                return False
            
            if api_type not in target_paths:
                return False
        return True
    
    @staticmethod
    def _pre_validation(result, method):
        """
        A method for validating after running method
        """
        pass

# COMMAND ----------

RequestBranchData.__dataclass_fields__["api_version"]

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
class RequestBranchData(RequestCommonData):
    base_path: str
    api_types: list      # e.g. ["daily_exports"]
    target_paths: dict   # e.g. {"daily_exports": "export"}

@dataclass
class ResponseCommonData:
    username: str
    password: str
    ip: str

@dataclass
class ResponseBranchData(ResponseCommonData):
    username: str
    password: str
    ip: str

# COMMAND ----------

# (!) ======================= TODO :: Get Params from Airflow

request_branch_data = RequestBranchData(
    advertiser="woori",
    base_url="api2.branch.io",
    base_db="auto_report",
    api_version="v3",
    channel="branch",
    base_path="/dbfs/FileStore/configs/",
    api_types=[
        "daily_exports"
    ],
    target_paths={
        "daily_exports": "export",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Data
# MAGIC - Source
# MAGIC   - `RequestBranchData`
# MAGIC - Naming
# MAGIC   - `{advertiser}{channel}Data`

# COMMAND ----------

class WooriBranchData:
    
    _CANDIDATES = [
        "advertiser",
        "base_url",
        "base_db",
        "api_version",
        "channel",
        "base_path",
        "api_types",
        "target_paths",
#         "config"
    ]
    
    def __init__(self, request_branch_data:RequestBranchData):
        data = {
            k: v for k, v in request_branch_data.__dict__.items() if k in WooriBranchData._CANDIDATES
        }
        self.__dict__.update(data)
        PtbwaUtils.check_result("WOORI-BRANCH-DATA(INIT)", self.__dict__)
    
    @DataValidator(do_pre_validation=False, do_post_validation=True)
    def _create(self):
        """
        A method creating additional data based on init
        validation:
            1. (optional) pre-conditions
            2. (optional) post-conditions
        """
        try:
            self.base_url = f"{self.base_url}/{self.api_version}"
            self.request_urls = {
                target_path: urlunparse(('https', self.base_url, target_path, None, None, None)) for target_path in self.target_paths
            }
            
            self.fields_path = os.path.join(self.base_path, f"{self.channel}_fields.csv")
            self.config_path = os.path.join(self.base_path, f"{self.channel}_api_client.pickle")
            
#             with open(self.config_path, "rb") as f:
#                 self.config = pickle.load(f)
            
            return self
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

woori_branch_data = WooriBranchData(request_branch_data)

# COMMAND ----------

woori_branch_data._create()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Validated Data

# COMMAND ----------

PtbwaUtils.check_result("VALIDATED-DATA", woori_branch_data.__dict__)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Save Credentials
# MAGIC - References
# MAGIC   - https://ptbwa.atlassian.net/wiki/spaces/DS/pages/82313248/AutoReport+Branch+API#%5CuD83D%5CuDCD8-Manual

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

# MAGIC %md
# MAGIC 
# MAGIC ##### Daily Exports Sample

# COMMAND ----------

# import requests

# url = "https://api2.branch.io/v3/export"

# payload = {
#     "branch_key": "asdf",
#     "branch_secret": "adf",
#     "export_date": "2022-07-15"
# }
# headers = {
#     "accept": "application/json",
#     "content-type": "application/json"
# }

# response = requests.post(url, json=payload, headers=headers)
# print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Double Type Fields

# COMMAND ----------

# user_data_prob_cross_platform_ids[].probability
# user_data_geo_lat
# user_data_geo_lon
# event_data_revenue
# event_data_revenue_in_usd
# event_data_exchange_rate
# event_data_shipping
# event_data_tax
# content_items[].dollar_price
# content_items[].dollar_price_in_usd
# content_items[].dollar_quantity
# content_items[].dollar_value
# content_items[].dollar_value_in_usd
# content_items[].dollar_rating
# content_items[].dollar_rating_average
# content_items[].dollar_rating_max
# content_items[].dollar_latitude
# content_items[].dollar_longitude

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### (Optional) Daily Exports Required Params
# MAGIC - branch_key
# MAGIC - branch_secret
# MAGIC - export_date

# COMMAND ----------

class WooriBranchAPI(AutoReportAPI):
    """
    A class for processing branch api procedures
    """
    def __init__(self, woori_branch_data:WooriBranchData) -> None:
        self.woori_branch_data = woori_branch_data
        
        self.fields = pd.read_csv(self.woori_branch_data.fields_path, delimiter=",", encoding="utf-8")
        self.current = datetime.now() + timedelta(hours=9)
        self.today = datetime.strftime(self.current, "%Y-%m-%d")
        self.yesterday = datetime.strftime(self.current - timedelta(days=1), "%Y-%m-%d")
        
        # (!) ===================== should be from WooriBranchData
        self.snaked_cols = []
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        required:
            2-times calls on daily-exports (default)
        """
        self._create_folder()
        self._extract_fields()
        
        for api_type in self.woori_branch_data.api_types:
            getattr(self, f"proc_{api_type}")(self.woori_branch_data.target_paths[api_type])
            
    def _create_folder(self):
        """
        A mehtod for creating folder to save raw data if not exist
        """
        try:
            folder_path = AutoReportUtils.create_path(
                "FileStore", 
                "autoreport", 
                self.woori_branch_data.advertiser, 
                self.yesterday
            )
            PtbwaUtils.check_result("FOLDER-PATH", folder_path)
            dbutils.fs.mkdirs(folder_path)
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def _extract_fields(self):
        """
        A mehtod for extracting fields(=metrics) from {branch_fields.csv} by specific entity
        """
        try:
            for col in self.fields.columns:
                snaked_col = BranchUtils.convert_to_snake(col)
                self.fields.rename(columns={col: snaked_col}, inplace=True)
                setattr(
                    self, 
                    f"{snaked_col}_fields", 
                    self.fields.query(f"{snaked_col} == 'y'")[["dimension", "format"]] # (!) pd.query()
                )
                self.snaked_cols.append(snaked_col)
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def proc_daily_exports(self, target_path):
        """
        A method for processing daily exports
        """
        try:
            PtbwaUtils.check_result(f"[BEFORE-CALL-GET-API]ATTRIBUTIONS::{self.__dict__}")
            
            # 1. post
            result = self.post_export(target_path)
            
            # 2. get
            self.get_export(result)
            
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
#     def proc_cohort(self, target_path):
#         """
#         A method for processing cohort
#         """
#         try:
#             pass
#         except Exception as e:
#             w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
#             raise e
    
    def get_export(self, result):
        """
        A method for getting {daily export}.csv from download urls
        args:
            result: from post_export()
        """
        try:
            for entity, download_urls in result.items()
                for i, download_url in enumerate(download_urls):
                    response = AutoReportUtils.get(
                        url=download_url
                    )
                    file_path = AutoReportUtils.create_path(
                        "dbfs",
                        "FileStore", 
                        "autoreport", 
                        self.woori_branch_data.advertiser, 
                        self.yesterday,
                        self.woori_branch_data.api_type,
                        f"{self.woori_branch_data.advertiser}_{snaked_col}_raw_{i}.csv"
                    )
                    with open(file_path, 'w') as f:
                        f.writelines(response.content.decode("utf-8"))
                        
#                     setattr(
#                         self,
#                         f"{snaked_col}_df", 
#                         (spark.read.format("csv").option("header", "true").schema(schema).load(file_path))
#                     )
                    
                    # (!) additional e.g. rename_cols, delete_prev, save_to_delta
                    
                    
                    
                    
    
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def post_export(self, target_path):
        """
        A mehtod for getting download-urls on {daily_exports}
        """
        try:
            method = sys._getframe().f_code.co_name
            
            # required params
#             "branch_key": "asdf",
#             "branch_secret": "adf",
#             "export_date": "2022-07-15"
            
            response = AutoReportUtils.post(
                url=self.woori_branch_data.request_urls[target_path], 
                method=method
            )
            result = response.json()
            PtbwaUtils.check_result("POST-EXPORT", result)
            return result
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# col = "pageview"
# isinstance(woori_branch_api.fields.query(f"{col} == 'y'")[["dimension", "format"]], pd.DataFrame)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

woori_branch_api = WooriBranchAPI(woori_branch_data)

# COMMAND ----------

try:
    woori_branch_api.proc_all()
except Exception as e:
    w_log.error("[FAIL-WOORI-BRANCH]")
    raise e

# COMMAND ----------

len(woori_branch_api.custom_event_fields)

# COMMAND ----------

len(woori_branch_api.click_fields)

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
# MAGIC #### Download CSV File Sample

# COMMAND ----------

# !wget -P /dbfs/ 'https://ab180-athena.s3.amazonaws.com/workgroup/airbridge-api/funble_2022-09-01_00%3A00%3A00%2B00%3A00-2022-09-26_00%3A00%3A00%2B00%3A00_jiho.lee%40ptbwa.com_20220926-0800_af0a352b-2b83-432c-8c0e-7699407fa0e6.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIA5YP6HDUGSYI6BRNT%2F20220926%2Fap-northeast-1%2Fs3%2Faws4_request&X-Amz-Date=20220926T080107Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Security-Token=IQoJb3JpZ2luX2VjED8aDmFwLW5vcnRoZWFzdC0xIkcwRQIhAKznyFjp1OK%2FlE05LaLUtTE0zpkfvS9pVQr2CUlNC9yMAiAf2FzSc3X5QbQBTQl7mmgmg%2BYk8adwpBcx2EyulSX5XyqbAwjp%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAMaDDk0NTk2MjgxODgyOSIMoNlQ0O6bTJcYob%2BLKu8Cggl%2BHVpmlyLQJ%2B%2F6oRMLcNO5qm3CBxWQ%2FZCYNicBHcCHrtLRcSLqoklPzlLz49Tw1t89y76oBet7T8j%2FGSVOPgdDYk3mRsStSyE51ukpD6A9jRZFA5FivEfJN7a3olZIGwCYDVjOhZF9AQuVhXrj4nUDQB9ECf7X%2F5%2BkmZEAsJbC1KC9d39jrqJfJqNkGyEhQYBGaq%2F92uB0z5ig%2FjQut5uFbbpLJN7ie9xJ8HOVcE9B8KP8WVYnXPloHbY8udHVjJ849keyqfWd9wSQEa42OCrkwDmZ%2BtRFfhO1sn2TC8lv5UJfM7Oz%2BTb4dwswOMQUFEDyMjiCV0Z%2B46mxEvb%2BZxjJPoxMwEPg4uUa6LZFJnFlYdocoPcFFrcjG8wI9EQkr3fGlZLbVBtuM9D6Aw58pul9sqEfzo11rUXEeqOxLxCeI%2B%2FCkJ%2BPmhflPG2KU2nTxnCfiNrasAn3nsis4zktMZqzekvvhrZMwVZO%2BZC5%2BjCursWZBjqdAXXPDlyYPS6pUwx1i3NxVeilG8fQP9Xt0Lk25uusFRZWKSXXhSszomtGR12xFMJLA7%2ByPP440kcON1GSZSAuVfSAD0ULp5rsAB7sxrToRUJRLNAgIH5JrNFvuHlmXkWvmpXFTjEI9WBfPZW3dFYC5UpdhlD6TWf3Ucwy76oYrPuulxiGMD1Fc77h2Kwxp4Q1KaZqoLGCVmxkgNdlplU%3D&X-Amz-Signature=997d14df11d6593f6f72560cb3a5a0828ad93a134c46fcbc65f738f766737bcd'