# Databricks notebook source
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("FUNBLE")

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone

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

KST = timezone('Asia/Seoul')
date = datetime.strftime((datetime.now(KST) - timedelta(1)), "%Y-%m-%d")
date

# COMMAND ----------

today = datetime.strftime((datetime.now(KST)), "%Y-%m-%d")
today

# COMMAND ----------

try:
    df = spark.sql(f"DELETE FROM {database}.funble_deduction_d WHERE date = '{date}'")
    log.info(f"[SUCCESS-DELETE-FUNBLE-REPORT-D]{df.collect()}")
except Exception as e:
    print(e)
    log.error(f"[FAIL-DELETE-FUNBLE-DEDUCTION-REPORT-D]{e}")
    raise e

# COMMAND ----------

try:
    insert_query = f"""
            INSERT INTO {database}.funble_deduction_d
                    SELECT
                        EventCategory,
                        Date,
                        EventDatetime,
                        EventName,
                        Channel,
                        CTIT,
                        AirbridgeDeviceID,
                        AirbridgeDeviceIDType,
                        DeviceType,
                        OSName,
                        OSVersion,
                        Country,
                        Language, 
                        NetworkCarrier,
                        CASE WHEN Country = 'kr' THEN TRUE ELSE FALSE END AS rule1,
                        CASE WHEN Language = 'ko' THEN TRUE ELSE FALSE END AS rule2,
                        CASE WHEN NetworkCarrier IN ('LGU+', 'SKT', 'LG U+', 'olleh', 'SKTelecom', 'KT', 'SK Telecom', 'KOR SK Telecom', 'LGU+ / 3G', 'KT / IoT network') THEN TRUE ELSE FALSE END AS rule3,
                        CASE WHEN rwAno = 1 AND rwDno = 1 THEN TRUE ELSE FALSE END AS rule4,
                        CASE WHEN CTIT >= 10 THEN TRUE ELSE FALSE END AS rule5
                    FROM (
                        SELECT
                            EventCategory,
                            DATE_FORMAT(SUBSTR(EventDatetime, 1, 10), 'yyyy-MM-dd') AS Date,
                            EventDatetime,
                            EventName,
                            Channel,
                            CTIT,
                            AirbridgeDeviceID,
                            AirbridgeDeviceIDType,
                            DeviceType,
                            OSName,
                            OSVersion,
                            Country,
                            Language, 
                            NetworkCarrier,
                            ROW_NUMBER() OVER (PARTITION BY AirbridgeDeviceID ORDER BY AirbridgeDeviceID, EventDatetime) AS rwAno,
                            ROW_NUMBER() OVER (PARTITION BY AirbridgeDeviceID ORDER BY AirbridgeDeviceID, EventDatetime DESC) AS rwDno
                        FROM {database}.airb_funble_mmp_raw
                        WHERE EventCategory = 'Install (App)'
                        AND Channel IN ('cauly', 'appier')
                        AND EventDatetime LIKE '{date}%'
                        )
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-FUNBLE-DEDUCTION-REPORT-D]{df.collect()}")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-FUNBLE-DEDUCTION-REPORT-D]{e}")
    raise e