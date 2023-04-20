# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Databricks Update Jobs
# MAGIC 
# MAGIC - Based on
# MAGIC   - `/Shared/autoreport/global/databricks-mgmt`
# MAGIC - Use Case
# MAGIC   - Airflow Pipeline Test

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Install Updated Version

# COMMAND ----------

# !sh /dbfs/FileStore/init_script/cp_module.sh

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
d_log = log4jLogger.LogManager.getLogger("DATABRICKS-UPDATE_JOBS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages
# MAGIC - Required `databricks-mgmt`

# COMMAND ----------

from databricks_mgmt import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - Required
# MAGIC   - env
# MAGIC   - project
# MAGIC - Optional
# MAGIC   - advertiser
# MAGIC     - Required `if project == "autoreport"`

# COMMAND ----------

try:
    # (!) LIVE
    env = dbutils.widgets.get("env")
    
    # (!) TEST
#     env = "dev"
except Exception as e:
    log.error("[FAIL-GET-PARAMS(DATABRICKS-UPDATE-JOBS)]REQUIRED::ENV")
    raise e

# COMMAND ----------

try:
    # (!) LIVE
    project = dbutils.widgets.get("project")
    
    # (!) TEST
#     project = "autoreport"
except Exception as e:
    log.error("[FAIL-GET-PARAMS(DATABRICKS-UPDATE-JOBS)]REQUIRED::PROJECT")
    raise e

# COMMAND ----------

try:
    # (!) LIVE
    advertisers = dbutils.widgets.get("advertisers")
    
    # (!) TEST
#     advertisers = "pcar"
except Exception as e:
    if project == "autoreport":
        log.error("[FAIL-GET-PARAMS(DATABRICKS-UPDATE-JOBS)]REQUIRED::ADVERTISERS")
        raise e

# COMMAND ----------

try:
    # (!) LIVE
    job_ids = dbutils.widgets.get("job_ids")
    
    # (!) TEST
#     job_ids = ["{job_id}"]
except Exception as e:
    if project == "autoreport":
        log.error("[FAIL-GET-PARAMS(DATABRICKS-UPDATE-JOBS)]REQUIRED::JOB_IDS")
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### TODO on Below Cells
# MAGIC - Add Get Params for Global Logic
# MAGIC - Remove Hard-Coded Value

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Notebooks

# COMMAND ----------

# databricks_api_manager = DatabricksAPIManager(
#     api_version="2.0", 
#     api_type="workspace",
#     target_path="/list", 
#     rest_type="get",
#     params={
#         "advertisers": advertisers.split(","),
#         "path": f"/Shared/{project}/"
#     }
# )

# COMMAND ----------

# results = databricks_api_manager.get_api_call(
#     project=project
# )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Job (Single)
# MAGIC - Filter Job IDs
# MAGIC   - Job IDs from Airflow

# COMMAND ----------

databricks_api_manager = DatabricksAPIManager(
    api_version="2.1", 
    api_type="jobs",
    target_path="/get", 
    rest_type="get",
)

# COMMAND ----------

jobs = databricks_api_manager.get_api_call(
    job_ids=job_ids.split(","),
    project=project,
)

# COMMAND ----------

jobs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Jobs (Multiple)
# MAGIC - Filter Job Names by
# MAGIC   - (Option-1) names
# MAGIC   - (Option-2) results
# MAGIC     - 1st-priority

# COMMAND ----------

# databricks_api_manager = DatabricksAPIManager(
#     api_version="2.1", 
#     api_type="jobs",
#     target_path="/list", 
#     rest_type="get",
#     params={
#         "limit": 25, # (!) max 25
#         "offset": 0,
# #         "names": ["autoreport-kcar"],
#     }
# )

# COMMAND ----------

# jobs = databricks_api_manager.get_api_call(
#     results=results,
#     project=project,
# )

# COMMAND ----------

# jobs

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

databricks_api_manager = DatabricksAPIManager(
    api_version="2.1", 
    api_type="jobs",
    target_path="/update", 
    rest_type="update",
)

# COMMAND ----------

databricks_api_manager.update_api_call(
    results=jobs,
    project=project,
    env=env,
    change_options=[
        "cluster"
    ],
)

# COMMAND ----------

# %sql

# GRANT CREATE ON SCHEMA ice TO `jinyoung.park@ptbwa.com`;