# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### AutoReport Washing
# MAGIC - Washing Common Processor
# MAGIC   - `2022-11-25` V1
# MAGIC - Using Widgets to Get Params from Airflow
# MAGIC   - Default
# MAGIC     - washing_period
# MAGIC     - washing_cycle
# MAGIC     - washing_day
# MAGIC     - notebook_name

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
w_log = log4jLogger.LogManager.getLogger("AUTOREPORT-WASHING")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install AttrDict
!python -m pip install pendulum

# COMMAND ----------

from datetime import datetime, timedelta
from dateutil import relativedelta
from attrdict import AttrDict
from enum import Enum
# from ptbwa_utils import DailyIterable

import pandas as pd
import pendulum
import requests
import time
import sys
import json
import pprint
import os

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - (1st) washing_preset
# MAGIC   - `last_month`
# MAGIC   - `last_week`
# MAGIC - (2nd) washing_period
# MAGIC   - `current - N-days` - `current - M-days`

# COMMAND ----------

# self.json["notebook_params"]["washing_params"] = json.dumps(self.json["notebook_params"]["washing_params"])
#         self.json["notebook_params"]["interval_s_date"] = self.interval_s_date.strftime("%Y-%m-%d")
#         self.json["notebook_params"]["interval_e_date"] = self.interval_e_date.strftime("%Y-%m-%d")

#         self.json["notebook_params"]["data_interval_start"] = \
#             context["data_interval_start"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

#         self.json["notebook_params"]["data_interval_end"] = \
#             context["data_interval_end"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

#         self.json["notebook_params"]["execution_date"] = \
#             context["execution_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

#         self.json["notebook_params"]["next_execution_date"] = \
#             context["next_execution_date"].in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

args = dict()

# COMMAND ----------

try:
    # (!) PRODUCTION
    args["washing_params"] = dbutils.widgets.get("washing_params")
    
    # (!) TEST
#     args["washing_params"] = json.dumps({
#         "notebook_name": "kcar-gad",
#         "washing_preset": "last_week",
#         "washing_period": "7|5",
#         "washing_cycle": "monthly",
#         "washing_day": "3"
#     })
except Exception as e:
    w_log.error("[FAIL-GET-WASHING-PARAMS]")
    raise e

# COMMAND ----------

try:
    # (!) PRODUCTION
    args["interval_s_date"] = dbutils.widgets.get("interval_s_date")

    # (!) TEST
#     args["next_execution_date"] = "2022-11-28 16:28:38"
except Exception as e:
    w_log.error("[FAIL-GET-DATA-INTERVAL-START]")
    raise e

# COMMAND ----------

try:
    # (!) PRODUCTION
    args["interval_e_date"] = dbutils.widgets.get("interval_e_date")

    # (!) TEST
#     args["next_execution_date"] = "2022-11-28 16:28:38"
except Exception as e:
    w_log.error("[FAIL-GET-ATA-INTERVAL-END]")
    raise e

# COMMAND ----------

# try:
#     # (!) PRODUCTION
#     args["s_date"] = dbutils.widgets.get("start_date")
    
# #     args["s_date"] = "2022-11-01"
#     # (!) TEST
# except Exception as e:
#     w_log.error("[FAIL-GET-START-DATE]")
#     raise e

# COMMAND ----------

# try:
#     # (!) PRODUCTION
#     args["e_date"] = dbutils.widgets.get("end_date")
    
#     # (!) TEST
# #     args["e_date"] = "2022-11-30"
# except Exception as e:
#     w_log.error("[FAIL-GET-END-DATE]")
#     raise e

# COMMAND ----------

args

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Class

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
            w_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

class AutoReportWashingProcessor:
    
    _VALID_WASHING_PRESETS = [
        "last_month",
        "last_week"
    ]
    
    def __init__(self, washing_args):
        self.args = washing_args
        self.args["washing_params"] = json.loads(self.args["washing_params"])
        self.args = AttrDict(self.args)
            
    def create_notebook_path(self):
        """
        A method for creating notebook path for using dbutils.notebook.run()
        """
        try:
            base_path = "/Shared/autoreport"
            adv_path = self.args.washing_params.notebook_name.split("-")[0]
            self.args["notebook_path"] = os.path.join(base_path, adv_path, self.args.washing_params.notebook_name)
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def wash(self):
        """
        A method for washing previous date on each (adv * channel) with day by day
        e.g. 2022-11-01 - 2022-11-03 :: for day in (11-01, 11-02)
        """
        try:
            # (!) option-1 :: run intervally e.g 3days
            dbutils.notebook.run(self.args.notebook_path, 0, {"s_date": self.args.interval_s_date, 
                                                              "e_date": self.args.interval_e_date})
            
            # (!) option-2 :: run day by day
#             for c_date in DailyIterable(self.args.s_date, self.args.e_date):
#                 # 0 means "no timeout"
#                 dbutils.notebook.run(self.args.notebook_path, 0, {"s_date": c_date, 
#                                                                   "e_date": c_date})
#                 time.sleep(30)
        except Exception as e:
            w_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

autoreport_washing_processor = AutoReportWashingProcessor(args)
autoreport_washing_processor.create_notebook_path()

# COMMAND ----------

autoreport_washing_processor.args

# COMMAND ----------

try:
    autoreport_washing_processor.wash()
except Exception as e:
    w_log.error(f"[FAIL-WASHING-PROCESSOR]{autoreport_washing_processor.args}")
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Notebook Run List
# MAGIC - by REST API

# COMMAND ----------

# databricks_instance = "dbc-024ee768-9ab2.cloud.databricks.com"
# api_version = "2.1"
# target_path = "jobs/runs/list"

# url = f"https://{databricks_instance}/api/{api_version}/{target_path}"

# COMMAND ----------

# headers = {"Authorization": "Bearer dapid2c8a4505f353f5545bb25350b1eb9da"} # (!) jinyoung.park@ptbwa.com PAT
# result = requests.get(url=url, headers=headers)

# COMMAND ----------

# result.json()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Error Handling

# COMMAND ----------

# def run_with_retry(notebook, timeout, args = {}, max_retries = 3):
#     num_retries = 0
#     while True:
#         try:
#             return dbutils.notebook.run(notebook, timeout, args)
#         except Exception as e:
#             if num_retries > max_retries:
#                 raise e
#             else:
#                 print("Retrying error", e)
#                 num_retries += 1
            
# run_with_retry("LOCATION_OF_CALLEE_NOTEBOOK", 60, max_retries = 5)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

# tmp = datetime.now() + timedelta(hours=9) - timedelta(days=5)

# COMMAND ----------

# tmp

# COMMAND ----------

# datetime(year=tmp.year, month=tmp.month - 1, day=1) + relativedelta.relativedelta(months=1)

# COMMAND ----------

# tmp.weekday()

# COMMAND ----------

# tmp - timedelta(days=tmp.weekday(), weeks=0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Date Range

# COMMAND ----------

# pd.date_range(start="2022-11-01", end="2022-11-30", freq="3D").tolist()