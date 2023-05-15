# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Kcar Criteo API
# MAGIC - Wiki
# MAGIC   - Criteo
# MAGIC     - https://ptbwa.atlassian.net/wiki/spaces/DS/pages/86179841/Criteo+API
# MAGIC   - Architecture
# MAGIC     - https://ptbwa.atlassian.net/wiki/spaces/DS/pages/84049921/AutoReport+Architecture+V2
# MAGIC - API Docs
# MAGIC   - https://developers.criteo.com/marketing-solutions/reference/apiv1oauth2tokenpost
# MAGIC - `2023-01-17` V1 | Init | @jinyoung.park

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
w_log = log4jLogger.LogManager.getLogger("KCAR-CRITEO")

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
    def delete_prev(table: str, date_col: str, start_date: str, end_date: str, **kwargs) -> None:
        """
        A method for deleting previous data to prevent duplication
        desc:
            table         e.g. {database}.{table_name}
            date_col      e.g. eventDate | dateStart | segmentDate (case-unchanged)
            start_date    e.g. 2022-10-01
            end_date      e.g. 2022-10-03
        """
        try:
            spark.sql(f"DELETE FROM {table} WHERE {date_col} BETWEEN '{start_date}' AND '{end_date}'")
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE::{table}")
            raise e
    
    @staticmethod
    def get(**kwargs) -> None:
        """
        A method for calling {get} api
        """
        try:
            response = requests.get(
                url=kwargs["url"],
                headers=CriteoUtils.create_request_headers(kwargs["method"]),
                json=CriteoUtils.create_request_params(kwargs["method"])
            )
            DatabricksAPIResponseValidator.is_valid(response)
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
            request_params = CriteoUtils.create_request_params(kwargs["method"])
            request_headers = CriteoUtils.create_request_headers(kwargs["method"])
            
            if kwargs["method"] in ["get_statistics_report"]:
                request_params["startDate"] = kwargs["start_date"]
                request_params["endDate"] = kwargs["end_date"]
                request_params = json.dumps(request_params)
            
            if "access_token" in kwargs:
                request_headers["authorization"] = f"Bearer {kwargs['access_token']}"
            
            response = requests.post(
                url=kwargs["url"],
                headers=request_headers,
                data=request_params
            )
            DatabricksAPIResponseValidator.is_valid(response)
            return response
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### CriteoUtils

# COMMAND ----------

class CriteoUtils:
    """
    A class for deling with repeated functions in criteo
    """ 
    
    _REQUEST_HEADERS = {
        "get_statistics_report": {
            "accept": "application/json",
            "content-type": "application/*+json"
        },
        "_get_access_token": {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        },
    }
    
    _REQUEST_PARAMS = {
        "get_statistics_report": {
            'dimensions': ['Ad', 'Campaign', 'Adset', 'Device', 'Channel', 'Day'],
            'metrics': ['Clicks', "Displays", "AdvertiserCost"],
            'timezone': 'UTC',
            'advertiserIds': '68643',
            'currency': 'KRW',
            'format': 'json',
#             'startDate': '2022-11-01',
#             'endDate': '2022-11-30'
        },
        "_get_access_token": "grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}",
    }
    
    @staticmethod
    def create_request_headers(method: str) -> dict:
        """
        A method for creating request headers based on {method} name e.g. post_export
        """
        try:
            request_headers = CriteoUtils._REQUEST_HEADERS[method]
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
            request_params = CriteoUtils._REQUEST_PARAMS[method]
            PtbwaUtils.check_result("REQUEST-PARAMS", request_params)
            return request_params
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    @staticmethod
    def create_table_path(advertiser, base_db, channel) -> str:
        """
        A method for creating delta table path e.g. stats
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
    def create_msg_format(func_name:str, status_code:int, errors:dict) -> str:
        return f"[FAIL-API-RESPONSE-VALIDATION({func_name}())]STATUS::{status_code}|ERRORS::{errors}"
    
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
            result = response.json()
            raise Exception(
                DatabricksAPIResponseValidator.create_msg_format(
                    sys._getframe().f_code.co_name,
                    response.status_code,
                    result["errors"],
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
        "fields_path": False,
        "config_path": True,
        "config": False,
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
            result: KcarCriteoData
        default:
            1. required
            2. type
            3. (optional) value
        """
        if isinstance(result, KcarCriteoData) and method in DataValidator._REQUIRED:
            # (!) required
            for required_key in DataValidator._REQUIRED[method]:
                if required_key not in result.__dict__:
                    raise ValueError(f"[NOT-FOUND-REQUIRED-KEY]{required_key}")
            
            # (!) type
            for k, v in result.__dict__.items():
                if k in RequestCriteoData.__dataclass_fields__ and not isinstance(v, RequestCriteoData.__dataclass_fields__[k].type):
                    raise ValueError(f"[TYPE-MISMATCH]KEY::{k}|TYPE::{RequestCriteoData.__dataclass_fields__[k].type}")
                
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
        return advertiser in ["woori", "webtoon-us", "kcar"]
    
    @staticmethod
    def is_valid_channel(channel: str):
        return channel in ["branch", "criteo"]
    
    @staticmethod
    def is_valid_base_path(base_path: str):
        return base_path.startswith("/dbfs")
    
    @staticmethod
    def is_valid_api_version(api_version: str):
        return re.compile("2022|2023-[0-9]+").match(api_version)
    
    @staticmethod
    def is_valid_api_types(api_types: list):
        for api_type in api_types:
            if api_type not in ["daily_exports", "cohort", "statistics_report", "access_token"]:
                return False
        return True
    
    @staticmethod
    def is_valid_target_paths(target_paths: dict):
        for target_path in target_paths.values():
            if target_path not in ["export", "cohort", "statistics/report", "oauth2/token"]:
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
            if key not in ["client_id", "client_secret", "application_id", "allowed_grant_types"]:
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
d_minus_two = datetime.strftime(datetime.now() + timedelta(hours=9) - timedelta(days=2), "%Y-%m-%d")

# COMMAND ----------

try:
    # (!) LIVE
    data_interval_end = AutoReportUtils.parse_date(dbutils.widgets.get("data_interval_end"), 1) # D-1 = YESTERDAY
    d_minus_two = AutoReportUtils.parse_date(dbutils.widgets.get("data_interval_end"), 2) # D-2
    
    # (!) TEST
#     data_interval_end = AutoReportUtils.parse_date("2023-03-02 14:00:00", 1)
except Exception as e:
    w_log.info("[FAIL-GET-DATA-INTERVAL-END-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

data_interval_end

# COMMAND ----------

d_minus_two

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
# MAGIC #### Request & Response
# MAGIC - @dataclass
# MAGIC   - Getter
# MAGIC   - Setter
# MAGIC - Required
# MAGIC   - Validation

# COMMAND ----------

# import requests

# url = "https://api.criteo.com/2022-10/statistics/report"

# # json dump
# payload = "{\"dimensions\":[\"Ad\",\"Campaign\",\"Adset\",\"Device\",\"Channel\",\"Day\"],\"metrics\":[\"Clicks\"],\"timezone\":\"Asia/Seoul\",\"advertiserIds\":\"68643\",\"currency\":\"USD\",\"format\":\"json\",\"startDate\":\"2022-11-01\",\"endDate\":\"2022-11-30\"}"
# headers = {
#     "accept": "application/json",
#     "content-type": "application/*+json",
#     "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IkZMNllURk1pcVliT1RGUW4yUmh3cWFLTnhuaGV1MFBJTjFlZU9ydnBoSWMiLCJ0eXAiOiJKV1QifQ.eyJjbGllbnRfaWQiOiI1YjI1MzEwOGMzNjc0OWM3YTM5ZmNjZWZmNmFhY2FlNCIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJpYXQiOjE2NzM5MzU3MTgsImV4cCI6MTY3MzkzNjY3OCwiaXNzIjoicHJpdmF0ZS1jcml0ZW8tZXhhbW9hdXRoIiwiYXVkIjoicHJpdmF0ZS1jcml0ZW8tZXhhbW9hdXRoIn0.FjDQkaz0cjFjqWO_7oPbjEM9-F9BvJVv7P_UsqRugTc6ctExIMZkBbN5z_rz526mo498pYiaUQTZDWDhR3BAXw_VcVv4lVclpCI3E-BPX7bN76MacGKEfJeN5tIoPukFElawTBl5VodeZ3yYlS2gKUxRNgohS-uBw7KWjpd_l-7ISsvLZTmtXicMUytiT9Pe7L4zIOdg8VHfsYqaXzr1M9nq39r3qQaXhvnZMrvG3zZeLDtKaB7YjMyTxwgnNX8CPDOtgnWs10eRCGIxMhkbDhftSiXa3LOptWL5mqtEf-L8i2keSrNcjmj2myr0l3G5_SoJeNvuIl9J1TRp9nw65g"
# }

# response = requests.post(url, data=payload, headers=headers)

# print(response.text)

# COMMAND ----------

@dataclass
class RequestCommonData:
    api_version: str
    base_url: str
    base_db: str
    advertiser: str
    channel: str

@dataclass
class RequestCriteoData(RequestCommonData):
    base_path: str
    api_types: list      # e.g. ["statistics_report"]
    target_paths: dict   # e.g. {"statistics_report": "statistics/report"}

@dataclass
class ResponseCommonData:
    username: str
    password: str
    ip: str

@dataclass
class ResponseCriteoData(ResponseCommonData):
    username: str
    password: str
    ip: str

# COMMAND ----------

request_criteo_data = RequestCriteoData(
    advertiser="kcar",
    base_url="api.criteo.com",
    base_db=database,
    api_version="2022-10",
    channel="criteo",
    base_path="/dbfs/FileStore/configs/",
    api_types=[
        "access_token",
        "statistics_report"
    ],
    target_paths={
        "access_token": "oauth2/token",
        "statistics_report": "statistics/report",
    }
)

# COMMAND ----------

request_criteo_data = RequestCriteoData(
    advertiser="kcar",
    base_url="api.criteo.com",
    base_db=database,
    api_version="2022-10",
    channel="criteo",
    base_path="/dbfs/FileStore/configs/",
    api_types=[
        "access_token",
        "statistics_report"
    ],
    target_paths={
        "access_token": "oauth2/token",
        "statistics_report": "statistics/report",
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Data
# MAGIC - Source
# MAGIC   - `RequestCriteoData`
# MAGIC - Naming
# MAGIC   - `{advertiser}{channel}Data`

# COMMAND ----------

class KcarCriteoData:
    
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
    
    def __init__(self, request_criteo_data:RequestCriteoData):
        data = {
            k: v for k, v in request_criteo_data.__dict__.items() if k in KcarCriteoData._CANDIDATES
        }
        self.__dict__.update(data)
        PtbwaUtils.check_result("KCAR-CRITEO-DATA(INIT)", self.__dict__)
    
    @DataValidator(do_pre_validation=False, do_post_validation=True)
    def _create(self):
        """
        A method creating additional data based on init
        validation:
            1. (optional) pre-conditions
            2. (optional) post-conditions
        """
        try:
            self.request_urls = {
                api_type: urlunparse(
                    (
                        'https', 
                        f"{self.base_url}/{self.api_version}" if api_type != "access_token" else self.base_url, 
                        self.target_paths[api_type], 
                        None, 
                        None, 
                        None
                    )
                ) for api_type in self.api_types
            }
            
            self.config_path = os.path.join(self.base_path, f"{self.channel}_api_client.pickle")
            
            with open(self.config_path, "rb") as f:
                self.config = pickle.load(f)
            
            return self
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

kcar_criteo_data = KcarCriteoData(request_criteo_data)

# COMMAND ----------

kcar_criteo_data._create()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Validated Data

# COMMAND ----------

PtbwaUtils.check_result("VALIDATED-DATA", kcar_criteo_data.__dict__)

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

class KcarCriteoAPI(AutoReportAPI):
    """
    A class for processing criteo api procedures
    """
    def __init__(self, kcar_criteo_data:KcarCriteoData) -> None:
        self.kcar_criteo_data = kcar_criteo_data
        
        self.current = datetime.now() + timedelta(hours=9)
        
        self.d_day = datetime.strftime(self.current, "%Y-%m-%d")
        self.d_minus_one = data_interval_end
        self.d_minus_two = d_minus_two         # data_interval_end - 1
        
    def proc_all(self):
        """
        A method for processing all methods orderly
        required:
            2-times calls on daily-exports (default)
        """
        
        for api_type in self.kcar_criteo_data.api_types:
            if api_type == "access_token":
                continue
            result = getattr(self, f"get_{api_type}")(api_type)
            
            json_rows = [json.dumps(row) for row in result["Rows"]]
            setattr(self, f"{api_type}_df", AutoReportUtils.create_json_rdd(json_rows))

            # (!) ================================================== 필요한 테이블만 저장하는 함수 생성
            
            table = CriteoUtils.create_table_path(
                advertiser=self.kcar_criteo_data.advertiser,
                base_db=self.kcar_criteo_data.base_db,
                channel=self.kcar_criteo_data.channel
            )
            PtbwaUtils.check_result("TABLE", table)
            
            AutoReportUtils.delete_prev(
                table=table,
                date_col="Day",
                start_date=self.d_minus_two,
                end_date=self.d_minus_one,
            )

            AutoReportUtils.save_to_delta(
                table=table,
                df=getattr(self, f"{api_type}_df"),
                mode="append"
            )
    
    def _get_access_token(self):
        """
        A method for getting {access token}
        """
        try:
            method = sys._getframe().f_code.co_name
            response = AutoReportUtils.post(
                url=self.kcar_criteo_data.request_urls["access_token"], 
                method=method
            )
            result = response.json()
            PtbwaUtils.check_result("ACCESS-TOKEN", result)
            return result
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_statistics_report(self, api_type):
        """
        A method for getting {statistics report}
        """
        try:
            oauth = self._get_access_token()
            method = sys._getframe().f_code.co_name
            response = AutoReportUtils.post(
                url=self.kcar_criteo_data.request_urls[api_type], 
                method=method,
                access_token=oauth["access_token"],
                start_date=self.d_minus_two,
                end_date=self.d_minus_one
            )
            result = response.json()
            PtbwaUtils.check_result("STATISTICS-REPORT", result)
            return result
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

kcar_criteo_api = KcarCriteoAPI(kcar_criteo_data)

# COMMAND ----------

kcar_criteo_api.d_minus_two

# COMMAND ----------

kcar_criteo_api.d_minus_one

# COMMAND ----------

try:
    kcar_criteo_api.proc_all()
except Exception as e:
    w_log.error("[FAIL-KCAR-CRITEO]")
    raise e
