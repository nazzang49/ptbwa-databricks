# Databricks notebook source
# MAGIC %md
# MAGIC ### Propfit Hourly Stat
# MAGIC * Processed Every 1- hour
# MAGIC * Saving Point
# MAGIC   * `cream.v_propfit_hourly`
# MAGIC * Version
# MAGIC   * Latest `2022-12-19`

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("PROPFIT-HOURLY")

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

KST = timezone('Asia/Seoul')

current_time = datetime.now(KST)
criteria_last_time = current_time - relativedelta(months=1)
criteria_last_date = criteria_last_time.replace(day=1).strftime("%Y-%m-%d") # 이전 월의 1일
criteria_next_time = current_time + relativedelta(months=2)
criteria_next_date = (criteria_next_time.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d") # 다음 월의 마지막 일

# print(current_time)
# print(criteria_last_date)
# print(criteria_next_date)

# COMMAND ----------

try:
    insert_query = f"""
            CREATE OR REPLACE VIEW cream.v_propfit_hourly
                AS (
                    SELECT
                        rh.actiontime_local_h AS Date,
                        ph.ExchangeTypes,
                        ph.ad_adv,
                        ph.PubId,
                        rh.url,
                        rh.appName,
                        rh.os,
                        rh.osv,
                        rh.hourly_dau,
                        rh.req,
                        rh.w AS Width,
                        rh.h AS Height,
                        ph.ad_grp,
                        ph.InventoryIdx,
                        ph.ad_cam,
                        ph.bid,
                        ph.imp,
                        ph.clk,
                        ph.Revenue,
                        b.Name AS CampaignName,
                        c.Name AS AdgroupName,
                        d.GoalBudget AS GoalBudget,
                        e.Name AS AdvertiserName,
                        rh.category01,
                        rh.category02
                    FROM (
                        SELECT
                            actiontime_local_h, year, month, day, hour, app_name AS appName, bundle AS url,  os, osv, hourly_dau, w, h,
                            g.category01, g.category02, request_count AS req
                        FROM cream.propfit_request_hourly r
                        LEFT JOIN (SELECT requestAppBundle, requestAppName, category01, category02 FROM ice.google_play_store_info_nhn) g
                            ON r.bundle = g.requestAppBundle
                        WHERE (year = YEAR('{current_time}') AND month IN (MONTH('{criteria_last_date}'), MONTH('{current_time}'), MONTH('{criteria_next_date}')))
                        ) rh
                    LEFT JOIN cream.propfit_hourly ph
                        ON ph.Date = rh.actiontime_local_h AND ph.url = rh.url AND ph.Width = rh.w AND ph.Height = rh.h AND (ph.Date BETWEEN '{criteria_last_date}' AND '{criteria_next_date}')
                    LEFT JOIN (SELECT CampaignIdx, Name, DailyBudget FROM ice.cm_campaign) b
                        ON ph.ad_cam = b.CampaignIdx
                    LEFT JOIN (SELECT CampaignIdx, AdgroupIdx, Name FROM ice.cm_adgroup) c
                        ON ph.ad_cam = c.CampaignIdx AND ph.ad_grp = c.AdgroupIdx
                    LEFT JOIN (SELECT Date, StartTime, CampaignIdx, AdgroupIdx, GoalBudget FROM ice.cm_adgroupschedule) d
                        ON ph.ad_cam = d.CampaignIdx AND ph.ad_grp = d.AdgroupIdx AND DATE(ph.Date) = d.Date AND DATE_FORMAT(ph.Date, 'HH:mm:ss') = d.StartTime
                    LEFT JOIN (SELECT AdvertiserInfoIdx, Name FROM ice.cm_advertiserinfo) e
                        ON ph.ad_adv = e.AdvertiserInfoIdx
                    )
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-REPLACE-VIEW-PROPFIT-HOURLY]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-REPLACE-VIEW-PROPFIT-HOURLY]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

