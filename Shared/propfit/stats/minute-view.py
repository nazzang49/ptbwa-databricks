# Databricks notebook source
# MAGIC %md
# MAGIC ### Save only 2 days Propfit 5-Minutes Stat
# MAGIC * Processed Every 5-minutes
# MAGIC * Saving Point
# MAGIC   * `cream.v_propfit_5min`
# MAGIC * Version
# MAGIC   * Latest `2022-12-05`

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("PROPFIT-MINUTES")

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone

# COMMAND ----------

KST = timezone('Asia/Seoul')
date = datetime.strftime((datetime.now(KST) - timedelta(2)), "%Y-%m-%d")
date

# COMMAND ----------

try:
    insert_query = f"""
            CREATE OR REPLACE VIEW cream.v_propfit_5min
                AS (
                    SELECT a.*, b.Name AS CampaignName, c.Name AS AdgroupName, d.GoalBudget AS GoalBudget, e.Name AS AdvertiserName
                    FROM cream.propfit_minutes a
                    LEFT JOIN (SELECT CampaignIdx, Name, DailyBudget FROM ice.cm_campaign) b
                        ON a.ad_cam = b.CampaignIdx
                    LEFT JOIN (SELECT CampaignIdx, AdgroupIdx, Name FROM ice.cm_adgroup) c
                        ON a.ad_cam = c.CampaignIdx AND a.ad_grp = c.AdgroupIdx
                    LEFT JOIN (SELECT Date, StartTime, CampaignIdx, AdgroupIdx, GoalBudget FROM ice.cm_adgroupschedule) d
                        ON a.ad_cam = d.CampaignIdx AND a.ad_grp = d.AdgroupIdx AND DATE(a.Date) = d.Date AND DATE_FORMAT(a.Date, 'HH:00:00') = d.StartTime
                    LEFT JOIN (SELECT AdvertiserInfoIdx, Name FROM ice.cm_advertiserinfo) e
                            ON a.ad_adv = e.AdvertiserInfoIdx
                    WHERE a.Date >= '{date}'
                    )
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-REPLACE-VIEW-PROPFIT-MINUTES]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-REPLACE-VIEW-PROPFIT-MINUTES]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

