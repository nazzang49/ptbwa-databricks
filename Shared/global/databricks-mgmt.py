# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Databricks Management
# MAGIC 
# MAGIC - Scope
# MAGIC   - API
# MAGIC   - dbutils
# MAGIC   - SQL

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
d_log = log4jLogger.LogManager.getLogger("DATABRICKS-MANAGEMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from functools import cached_property
from datetime import datetime, timedelta
from delta.tables import *

import pprint
import sys
import os
import json
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Utils
# MAGIC - Global
# MAGIC - Validator
# MAGIC - Databricks-Centric

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
            d_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

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
    def has_more(response):
        """
        A method for validating "*/list" APIs
        """
        result = response.json()
        if "has_more" in result and not result["has_more"] and len(result.keys()) == 1:
            custom_msg = DatabricksAPIResponseValidator.create_msg_format(
                sys._getframe().f_code.co_name, 
                response.status_code, 
                "NO_DATA", 
                f"TARGET[{response.url}]|CHECK-NAME-FILTER-CONDITION"
            )
            d_log.warn(custom_msg)
            print(custom_msg)
            return False
        return True

# COMMAND ----------

class DatabricksUtils:
    
    @staticmethod
    def create_job_cluster_keys(results, project: str):
        return {result[1]: f"{result[1]}_cluster" for result in results}
    
    @staticmethod
    def create_job_names_notebook_paths(results, project: str):
        job_names, notebook_paths = [], []
        for result in results:
            for notebook in result["objects"]:
                notebook_name = notebook["path"].split("/")[-1]
                splitted_notebook_name = notebook_name.split("-")
                splitted_notebook_name.insert(0, "autoreport") # e.g. if env == 'dev' => tt-{project}-{notebook_name}
                
                job_names.append('-'.join(splitted_notebook_name))
                notebook_paths.append(notebook["path"])
        return job_names, notebook_paths
      
    @staticmethod
    def get_params(**kwargs):
        if "project" not in kwargs or "env" not in kwargs or "func_name" not in kwargs:
            raise ValueError("[NOT-FOUND-ARGS]REQUIRED::{project, env, func_name}")
        
        base_path = "/dbfs/FileStore/configs/"
        file_name = f"{kwargs['project']}_{kwargs['func_name']}_{kwargs['env']}.json"
        json_path = os.path.join(base_path, file_name) # e.g. autoreport_post_jobs_dev.json
        with open(json_path, "r") as f:
            params = json.load(f)
        return params
    
    @staticmethod
    def change_cluster(params: dict, **kwargs):
        if "env" not in kwargs:
            raise ValueError("[NOT-FOUND-ENV]REQUIRED")
            
        if kwargs["env"] == "dev":
            params["new_settings"]["tasks"][0]["existing_cluster_id"] = "1026-083605-h88ik7f2" # All-Purpose
        else:
            job_name = params["new_settings"]["tasks"][0]["task_key"]
            if job_name not in kwargs["job_cluster_keys"]:
                raise ValueError(f"[NOT-FOUND-JOB-CLUSTER-KEY]JOB-NAME::{job_name}")
            params["new_settings"]["tasks"][0]["job_cluster_key"] = kwargs["job_cluster_keys"][job_name]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### API

# COMMAND ----------

class DatabricksAPIManager:
    
    _REST_TYPE_CANDIDATES = [
        "get",
        "post",
        "update",
        "delete",
        "repair"
    ]
    
    def __init__(self, *args, **kwargs):
        # (!) required
        self._pat = kwargs["pat"] if "pat" in kwargs else "dapid2c8a4505f353f5545bb25350b1eb9da" # jyp
        self._api_type = kwargs["api_type"] if "api_type" in kwargs else "workspace"
        self._base_url = kwargs["base_url"] if "base_url" in kwargs else "https://dbc-024ee768-9ab2.cloud.databricks.com/api"
        self._api_version = kwargs["api_version"] if "api_version" in kwargs else "2.1"
        self._rest_type = kwargs["rest_type"] if "rest_type" in kwargs else "get"
        self._params = kwargs["params"] if "params" in kwargs else {}
        
        # (!) optional
        self._advertisers = self._params.pop("advertisers") if "advertisers" in self._params else []
        
        if "target_path" not in kwargs:
            raise ValueError("[NOT-FOUND-TARGET-PATH]REQUIRED")
            
        self._target_path = self._api_type + kwargs["target_path"]
    
    @cached_property
    def headers(self):
        return {"Authorization": f"Bearer {self._pat}"}
    
    @cached_property
    def base_url(self):
        return self._base_url
    
    @cached_property
    def api_version(self):
        return self._api_version
    
    @cached_property
    def request_url(self):
        return f"{self._base_url}/{self._api_version}/{self._target_path}"
    
    @cached_property
    def rest_type(self):
        if self._rest_type not in DatabricksAPIManager._REST_TYPE_CANDIDATES:
            raise ValueError(
                f"[NOT-FOUND]REST-TYPE::{self.rest_type}|CANDIDATES::{DatabricksAPIManager._REST_TYPE_CANDIDATES}"
            )
        return self._rest_type
    
    @cached_property
    def advertisers(self):
        return self._advertisers
    
    @cached_property
    def params(self):
        return self._params

    @cached_property
    def api_type(self):
        return self._api_type
    
    def get_api_call(self, **kwargs):
        """
        A method for getting-related api calls on databricks
        """
        try:
            # (!) multiple vs single
            func_name = f"{self.rest_type}_{self.api_type}" if "/list" in self._target_path else f"{self.rest_type}_{self.api_type[:-1]}"
            results = getattr(self, func_name)(**kwargs)
            PtbwaUtils.check_result(func_name.upper(), results)
            return results
        except ValueError as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]VALUE-ERROR::{e}")
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def create_api_call(self, **kwargs):
        """
        A method for creation-related api calls on databricks
        """
        try:    
            func_name = f"{self.rest_type}_{self.api_type}"
            results = getattr(self, func_name)(**kwargs)
            PtbwaUtils.check_result(func_name.upper(), results)
            return results
        except ValueError as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]VALUE-ERROR::{e}")
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def delete_api_call(self, **kwargs):
        """
        A method for deletion-related api calls on databricks
        """
        try:    
            func_name = f"{self.rest_type}_{self.api_type}"
            results = getattr(self, func_name)(**kwargs)
            PtbwaUtils.check_result(func_name.upper(), results)
            return results
        except ValueError as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]VALUE-ERROR::{e}")
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def update_api_call(self, **kwargs):
        """
        A method for update-related api calls on databricks
        """
        try:    
            func_name = f"{self.rest_type}_{self.api_type}"
            results = getattr(self, func_name)(**kwargs)
            PtbwaUtils.check_result(func_name.upper(), results)
            return results
        except ValueError as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]VALUE-ERROR::{e}")
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def get_workspace(self, **kwargs):
        """
        A method for getting workspace results
        """
        path = self.params.pop("path")
        results = []
        for advertiser in self.advertisers:
            advertiser_dir = path + advertiser
            self.params["path"] = advertiser_dir
            
            results.append(
                requests.get(url=self.request_url, headers=self.headers, params=self.params).json()
            )
        return results
    
    def get_job(self, **kwargs):
        """
        A method for getting job result (single) by job id
        """
        if "job_ids" not in kwargs:
            raise ValueError("[NOT-FOUND-JOB-IDS]REQUIRED")
        
        results = []
        for job_id in kwargs["job_ids"]:
            self.params["job_id"] = job_id
            response = requests.get(url=self.request_url, headers=self.headers, params=self.params)
            PtbwaUtils.check_result("JOB", response.json())
            results.append(response.json())

        candidate_jobs = [
            (job["job_id"], job["settings"]["name"]) for job in results
        ]
        return candidate_jobs
    
    def get_jobs(self, **kwargs):
        """
        A method for getting job results (multiple) by job name
        """
        job_names = []
        # (!) from user
        if "names" in self.params:
            job_names = self.params.pop("names")
            PtbwaUtils.check_result("JOB-NAMES", job_names)
        
        # (!) from get_workspace
        if "results" in kwargs:
            job_names, notebook_paths = DatabricksUtils.create_job_names_notebook_paths(
                kwargs["results"], 
                kwargs["project"],
            )
            PtbwaUtils.check_result("JOB-NAMES", job_names)
            PtbwaUtils.check_result("NOTEBOOK-PATHS", notebook_paths)
        
        results = []
        for job_name in job_names:
            self.params["name"] = job_name
            response = requests.get(url=self.request_url, headers=self.headers, params=self.params)
            if DatabricksAPIResponseValidator.has_more(response):
                results.append(response.json())
            
        candidate_jobs = [
            (job["job_id"], job["settings"]["name"]) for result in results for job in result["jobs"]
        ]
        return candidate_jobs
    
    def post_jobs(self, **kwargs):
        """
        A method for creating new jobs
        """
        if "results" not in kwargs:
            raise ValueError("[NOT-FOUND-RESULTS]REQUIRED")
        
        kwargs["func_name"] = sys._getframe().f_code.co_name
        job_names, notebook_paths = DatabricksUtils.create_job_names_notebook_paths(
            kwargs["results"], 
            kwargs["project"],
        )
        PtbwaUtils.check_result("JOB-NAMES", job_names)
        PtbwaUtils.check_result("NOTEBOOK-PATHS", notebook_paths)
        
        params = DatabricksUtils.get_params(**kwargs)
        
        jobs = []
        for job_name, notebook_path in zip(job_names, notebook_paths):
            params["name"] = job_name
            params["tasks"][0]["task_key"] = job_name
            params["tasks"][0]["notebook_task"]["notebook_path"] = notebook_path
            params["tags"]["project"] = kwargs["project"]
            params["tags"]["env"] = kwargs["env"]
            PtbwaUtils.check_result("PARAMS", params)
            
            job = requests.post(url=self.request_url, headers=self.headers, json=params).json()
            job["job_name"] = job_name
            job["notebook_path"] = notebook_path
            jobs.append(job)
        return jobs
    
    def update_jobs(self, **kwargs):
        """
        A method for updating job settings
        """
        if "results" not in kwargs:
            raise ValueError("[NOT-FOUND-RESULTS]REQUIRED")
        
        if "change_options" not in kwargs:
            raise ValueError("[NOT-FOUND-CHANGE-OPTIONS]REQUIRED")
        
        if kwargs["env"] == "prod":
            kwargs["job_cluster_keys"] = DatabricksUtils.create_job_cluster_keys(
                kwargs["results"],
                kwargs["project"],
            )
            PtbwaUtils.check_result("JOB-CLUSTER-KEYS", kwargs["job_cluster_keys"])
        
        kwargs["func_name"] = sys._getframe().f_code.co_name
        params = DatabricksUtils.get_params(**kwargs)
            
        jobs = []
        for result in kwargs["results"]:
            params["job_id"] = result[0]
            params["new_settings"]["tasks"][0]["task_key"] = result[1]
            
            for change_option in kwargs["change_options"]:
                getattr(DatabricksUtils, f"change_{change_option}")(params, **kwargs)
            PtbwaUtils.check_result("PARAMS", params)
            
            job = requests.post(url=self.request_url, headers=self.headers, json=params).json()
            job["job_id"] = result[0]
            jobs.append(job)
    
    def delete_jobs(self, **kwargs):
        """
        A method for deleting created jobs
        """
        if "job_ids" not in kwargs:
            raise ValueError("[NOT-FOUND-JOB-IDS]REQUIRED")
        
        jobs = []
        for job_id in kwargs["job_ids"]:
            jobs.append(
                requests.post(url=self.request_url, headers=self.headers, json={"job_id": job_id}).json()
            )
        return jobs
      
    def repair_jobs(self, **kwargs):
        """
        A method for repairing failed runs
        """
        if "run_ids" not in kwargs:
            raise ValueError("[NOT-FOUND-RUN-IDS]REQUIRED")
            
        runs = []
        for run_id in kwargs["run_ids"]:
            runs.append(
                requests.post(url=self.request_url, headers=self.headers, json={"run_id": run_id, "rerun_all_failed_tasks": True}).json()
            )
        return runs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Update Jobs Params Sample
# MAGIC - dev
# MAGIC - prod

# COMMAND ----------

# (!) dev (pool => all-purpose)
# d = {
#   "job_id": "{job_id}", # unique
#   "new_settings": {
#     "tasks": [
#       {
#         "task_key": "{job_name}", # unique
#         "existing_cluster_id": "0923-164208-meows279",
#       }
#     ]
#   }
# }

# COMMAND ----------

# (!) prod (all-purpose => pool)
# d = {
#   "job_id": "{job_id}",
#   "new_settings": {
#     "tasks": [
#       {
#         "task_key": "{job_name}",
#         "job_cluster_key": "{job_cluster_key}", # e.g. {job_name}_cluster
#       },
#     ]
#   }
# }

# COMMAND ----------

# with open("/dbfs/FileStore/configs/autoreport_update_jobs_prod.json", "w") as f:
#     json.dump(d, f)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Post Jobs Params Sample

# COMMAND ----------

# d = {'email_notifications': {'no_alert_for_skipped_runs': 'false',
#                                                 'on_failure': ['z3k2u1l1u8v5x3t4@performancebytbwa.slack.com']}, # jyp
#                      'format': 'MULTI_TASK',
#                      'max_concurrent_runs': 2,
#                      'name': "",
#                      'tags': {'advertiser': 'test',
#                               'channel': 'test',
#                               'env': 'dev',
#                               'project': 'autoreport',
#                               'schedule': 'triggered',
#                               'type': 'type'},
#                      'tasks': [{'task_key': "",
#                                 'email_notifications': {},
#                                 'existing_cluster_id': '1026-083605-h88ik7f2',
#                                 'notebook_task': {'notebook_path': "",
#                                                   'source': 'WORKSPACE'},
#                                 'timeout_seconds': 0}],
#                      'timeout_seconds': 0,
#                      'webhook_notifications': {}}

# COMMAND ----------

# pprint.pprint(d)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Notebooks

# COMMAND ----------

databricks_api_manager = DatabricksAPIManager(
    api_version="2.0", 
    api_type="workspace",
    target_path="/list", 
    rest_type="get",
    params={
        "advertisers": ["kcar"],
        "path": "/Shared/autoreport/"
    }
)

# COMMAND ----------

results = databricks_api_manager.get_api_call(
    project="autoreport"
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Repair Runs

# COMMAND ----------

databricks_api_manager = DatabricksAPIManager(
    api_version="2.1", 
    api_type="jobs",
    target_path="/runs/repair", 
    rest_type="repair",
)

# COMMAND ----------

results = databricks_api_manager.update_api_call(
    run_ids=[
        "73630893"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Jobs
# MAGIC - Required Notebooks

# COMMAND ----------

# databricks_api_manager = DatabricksAPIManager(
#     api_version="2.1", 
#     api_type="jobs",
#     target_path="/create", 
#     rest_type="post",
# )

# COMMAND ----------

# databricks_api_manager.create_api_call(
#     results=results,
#     project="autoreport",
#     env="dev"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Jobs (Multiple)
# MAGIC - Filter Job Names by
# MAGIC   - (Option-1) names
# MAGIC   - (Option-2) results
# MAGIC     - 1st-priority

# COMMAND ----------

databricks_api_manager = DatabricksAPIManager(
    api_version="2.1", 
    api_type="jobs",
    target_path="/list", 
    rest_type="get",
    params={
        "limit": 25, # (!) max 25
        "offset": 0,
#         "names": ["autoreport-kcar"],
    }
)

# COMMAND ----------

jobs = databricks_api_manager.get_api_call(
    results=results,
    project="autoreport",
)

# COMMAND ----------

jobs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Job (Single)
# MAGIC - Filter Job IDs

# COMMAND ----------

databricks_api_manager = DatabricksAPIManager(
    api_version="2.1", 
    api_type="jobs",
    target_path="/get", 
    rest_type="get",
)

# COMMAND ----------

jobs = databricks_api_manager.get_api_call(
    job_ids=[
        "1017170494348786"
    ],
    project="autoreport",
)

# COMMAND ----------

jobs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Get Jobs Test

# COMMAND ----------

response = requests.get(
    url="https://dbc-024ee768-9ab2.cloud.databricks.com/api/2.1/jobs/get", 
    headers={"Authorization": f"Bearer dapid2c8a4505f353f5545bb25350b1eb9da"}, 
    params={
        "job_id": "1017170494348786"
    }
)

# COMMAND ----------

response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Delete Jobs
# MAGIC - Required Jobs

# COMMAND ----------

# databricks_api_manager = DatabricksAPIManager(
#     api_version="2.1", 
#     api_type="jobs",
#     target_path="/delete", 
#     rest_type="delete",
# )

# COMMAND ----------

# databricks_api_manager.delete_api_call(
#     job_ids=[
#         job[0] for job in jobs
#     ]
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Update Jobs
# MAGIC - Required Jobs
# MAGIC - Use Case 
# MAGIC   - When Airflow Test
# MAGIC     - `if env == "dev"`
# MAGIC       - Change Cluster into `test :: All-Purpose`
# MAGIC     - `if env == "prod"`
# MAGIC       - Change Cluster into `pool`

# COMMAND ----------

# databricks_api_manager = DatabricksAPIManager(
#     api_version="2.1", 
#     api_type="jobs",
#     target_path="/update", 
#     rest_type="update",
# )

# COMMAND ----------

# databricks_api_manager.update_api_call(
#     results=jobs,
#     project="autoreport",
#     env="prod",
#     change_options=[
#         "cluster"
#     ],
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SQL

# COMMAND ----------

class DatabricksSQLBuilder:
    
    _SQL_TYPE_CANDIDATES = [
        "ddl",
        "dml",
        "dcl",
    ]
    
    
    """
    A class for creating queries to do SQL process
    """
    def __init__(self, *args, **kwargs):
        self._sql_type = kwargs["sql_type"] if "sql_type" in kwargs else "dml"
        self._sql_sub_type = kwargs["sql_sub_type"] if "sql_sub_type" in kwargs else "select"
        self._option = kwargs["option"] if "option" in kwargs else "default"
        self._params = kwargs["params"] if "params" in kwargs else {}
    
    @cached_property
    def sql_type(self):
        """
        e.g. dml
        """
        return self._sql_type
    
    @cached_property
    def sql_sub_type(self):
        """
        e.g. select
        """
        return self._sql_sub_type
    
    @cached_property
    def params(self):
        """
        e.g. database
        """
        return self._params

    @cached_property
    def option(self):
        """
        e.g. clone
        """
        return self._option
    
    def create_queries(self, **kwargs):
        """
        A method for creating queries[n >= 1] based on ddl, dml, dcl
        """
        queries = getattr(self, f"do_{self.sql_type}")(**kwargs)
        PtbwaUtils.check_result("QUERIES", queries)
        return queries
    
    def do_ddl(self, **kwargs):
        return getattr(self, f"{self.sql_sub_type}_by_{self.option}")(**kwargs)
    
    def do_dml(self, **kwargs):
        pass
    
    def do_dcl(self, **kwargs):
        pass
    
    def show_tables_by_default(self, **kwargs):
        try:
            if "params" not in kwargs:
                raise ValueError("[NOT-FOUND-PARAMS]REQUIRED")
            
            if "database" not in kwargs["params"]:
                raise ValueError("[NOT-FOUND-DATABASE]REQUIRED")
                
            if not kwargs["params"]["database"]:
                raise ValueError("[EMPTY-DATABASE]REQUIRED")
            
            return [f"SHOW tables IN {database}" for database in kwargs["params"]["database"]]
        except ValueError as e:
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]QUERY::{query}")
            raise e
    
    def show_views_by_default(self, **kwargs):
        try:
            if "params" not in kwargs:
                raise ValueError("[NOT-FOUND-PARAMS]REQUIRED")
            
            if "database" not in kwargs["params"]:
                raise ValueError("[NOT-FOUND-DATABASE]REQUIRED")
                
            if not kwargs["params"]["database"]:
                raise ValueError("[EMPTY-DATABASE]REQUIRED")
            
            return [f"SHOW views IN {database}" for database in kwargs["params"]["database"]]
        except ValueError as e:
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]QUERY::{query}")
            raise e
    
    def create_tables_by_clone(self, **kwargs):
        """
        A method for creating table by clone queries based on results of "show_tables"
        """
        try:
            if "params" not in kwargs:
                raise ValueError("[NOT-FOUND-PARAMS]REQUIRED")
            
            if "results" not in kwargs["params"] or "dst_database" not in kwargs["params"]:
                raise ValueError("[NOT-FOUND-RESULTS|DST_DATABASE]REQUIRED")
                
            if not kwargs["params"]["results"] or not kwargs["params"]["dst_database"]:
                raise ValueError("[EMPTY-RESULTS|DST_DATABASE]REQUIRED")
                
            results = kwargs["params"]["results"]
            dst_database = kwargs["params"]["dst_database"]
                
            return [
                f"CREATE TABLE IF NOT EXISTS {dst_database}.{result['tableName']} CLONE {result['database']}.{result['tableName']}" for result in results
            ]
        except ValueError as e:
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]QUERY::{query}")
            raise e

# COMMAND ----------

class DatabricksSQLManager:
    
    _IS_PREPROCESS = {
        "show_tables": True,
        "show_views": False,
        "create_tables_by_clone": False
    }
    
    _CUSTOM_ARGS_CANDIDATES = [
        "sql_sub_type",
        "queries",
        "views"
    ]
    
    _SELECT_COLUMNS = {
        "show_tables": [
            "tableName", 
            "database"
        ],
        "create_tables_by_clone": [
            "source_table_size",
            "copied_files_size"
        ],
        "show_views": [
            "viewName", 
            "namespace"
        ],
    }
    
    def __init__(self, *args, **kwargs):
        custom_args = {
            k: v for k, v in kwargs.items() if k in DatabricksSQLManager._CUSTOM_ARGS_CANDIDATES
        }
        self.__dict__.update(custom_args)
        
        if not self.queries:
            raise ValueError("[EMPTY-QUERIES]REQUIRED")

    def do_queries(self, **kwargs):
        """
        A method for doing queries from SQL-Builder
            1. (Optional) Preprocess
            2. Do Queries
            3. Postprocess
        """
        try:
            if "sql_sub_type" not in kwargs:
                raise ValueError("[NOT-FOUND-SQL-SUB-TYPE]REQUIRED")
            
            func_key = f"{kwargs['sql_sub_type']}" if "option" not in kwargs else f"{kwargs['sql_sub_type']}_by_{kwargs['option']}"
            if DatabricksSQLManager._IS_PREPROCESS[func_key]:
                pre_process_func_name = f"{func_key}_preprocess"
                getattr(self, pre_process_func_name)(**kwargs)
            
            kwargs["results"] = {
                query: spark.sql(query).select(DatabricksSQLManager._SELECT_COLUMNS[func_key]).collect() for query in self.queries
            }
            PtbwaUtils.check_result("QUERIES-EXECUTION", kwargs["results"])
            
            post_process_func_name = f"{func_key}_postprocess"
            return getattr(self, post_process_func_name)(**kwargs)
        except ValueError as e:
            raise e
        except Exception as e:
            d_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def show_tables_preprocess(self, **kwargs):
        """
        A method for 
        """
        if "views" not in self.__dict__:
            raise ValueError("[NOT-FOUND-VIEWS]REQUIRED")
        pass
        
    def show_tables_postprocess(self, **kwargs):
        if "results" not in kwargs:
            raise ValueError("[NOT-FOUND-RESULTS]REQUIRED")
        
        if "views" not in self.__dict__:
            raise ValueError("[NOT-FOUND-VIEWS]REQUIRED")
        
        tables = [
            {col:row[col] for col in DatabricksSQLManager._SELECT_COLUMNS["show_tables"]} for result in kwargs["results"].values() for row in result
        ]
        
        # (!) exclude views
        view_names = [view["viewName"] for view in self.views]
        return [table for table in tables if table["tableName"] not in view_names]
    
    def show_views_postprocess(self, **kwargs):
        if "results" not in kwargs:
            raise ValueError("[NOT-FOUND-RESULTS]REQUIRED")
            
        return [
            {col:row[col] for col in DatabricksSQLManager._SELECT_COLUMNS["show_views"]} for result in kwargs["results"].values() for row in result
        ]
    
    def create_tables_by_clone_postprocess(self, **kwargs):
        if "results" not in kwargs:
            raise ValueError("[NOT-FOUND-RESULTS]REQUIRED")
            
        return [
            {query: row} for query, result in kwargs["results"].items() for row in result
        ]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 1. SQL - Show Views

# COMMAND ----------

databricks_sql_builder = DatabricksSQLBuilder(
    sql_type="ddl",
    sql_sub_type="show_views",
    option="default",
    params={
        "database": [
            "ice"
        ]
    }
)

# COMMAND ----------

queries = databricks_sql_builder.create_queries(
    params={
        "database": [
            "ice", 
        ]
    }
)

# COMMAND ----------

databricks_sql_manager = DatabricksSQLManager(
    queries=queries,
)

# COMMAND ----------

views = databricks_sql_manager.do_queries(
    sql_sub_type=databricks_sql_builder.sql_sub_type
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### View Results Sample

# COMMAND ----------

#  {'namespace': 'auto_report', 'viewName': 'tv_nsa_kakaopay_keyword_stats_d'},
#  {'namespace': 'auto_report', 'viewName': 'tv_fb_kakaopay_ad_stats_d'},
#  {'namespace': 'auto_report', 'viewName': 'tv_gad_kakaopay_advideo_stats_d'}]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 2. SQL - Show Tables

# COMMAND ----------

databricks_sql_builder = DatabricksSQLBuilder(
    sql_type="ddl",
    sql_sub_type="show_tables",
    option="default",
    params={
        "database": [
            "ice"
        ]
    }
)

# COMMAND ----------

queries = databricks_sql_builder.create_queries(
    params={
        "database": [
            "ice", 
        ]
    }
)

# COMMAND ----------

databricks_sql_manager = DatabricksSQLManager(
    queries=queries,
    views=views # (!) exclude views
)

# COMMAND ----------

results = databricks_sql_manager.do_queries(
    sql_sub_type=databricks_sql_builder.sql_sub_type
)

# COMMAND ----------

results

# COMMAND ----------

len(results)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Extract Specific Tables
# MAGIC - Use Case

# COMMAND ----------

filtered_results = [result for result in results if "streams" in result["tableName"]]

# COMMAND ----------

filtered_results
# filtered_results = filtered_results[3:5]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 3. Create by Clone Test
# MAGIC - Required `[Database.Table]`

# COMMAND ----------

databricks_sql_builder = DatabricksSQLBuilder(
    sql_type="ddl",
    sql_sub_type="create_tables",
    option="clone",
)

# COMMAND ----------

queries = databricks_sql_builder.create_queries(
    params={
        "results": filtered_results,
        "dst_database": "tt_ice"
    }
)

# COMMAND ----------

databricks_sql_manager = DatabricksSQLManager(
    queries=queries,
)

# COMMAND ----------

results = databricks_sql_manager.do_queries(
    sql_sub_type=databricks_sql_builder.sql_sub_type,
    option=databricks_sql_builder.option,
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Decorator for Validation
# MAGIC - PRE
# MAGIC - POST

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Package
# MAGIC - Required Process Shell Script

# COMMAND ----------

# script = """
# #!/bin/bash
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/date_function.py /databricks/python3/lib/python3.8/site-packages/date_function.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/etc_function.py /databricks/python3/lib/python3.8/site-packages/etc_function.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/func1.py /databricks/python3/lib/python3.8/site-packages/func1.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/mysql_function.py /databricks/python3/lib/python3.8/site-packages/mysql_function.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/s3_function.py /databricks/python3/lib/python3.8/site-packages/s3_function.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/ptbwa_utils.py /databricks/python3/lib/python3.8/site-packages/ptbwa_utils.py
# cp /dbfs/mnt/ptbwa-basic/tbwa_custommodule/databricks_mgmt.py /databricks/python3/lib/python3.8/site-packages/databricks_mgmt.py
# """

# COMMAND ----------

# dbutils.fs.put("dbfs:/FileStore/init_script/cp_module.sh", script, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### ETC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Check Datadog Install Script

# COMMAND ----------

!cat /dbfs/FileStore/init_script/datadog-install-driver-only.sh

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Check DLT EventLog

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

