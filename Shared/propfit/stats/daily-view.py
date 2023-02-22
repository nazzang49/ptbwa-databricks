# Databricks notebook source
# MAGIC %md
# MAGIC ### Propfit Daily Stat
# MAGIC * Processed Every 1- day
# MAGIC * Saving Point
# MAGIC   * `cream.v_propfit_daily`
# MAGIC * Version
# MAGIC   * Latest `2022-12-19`

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("PROPFIT-DAILY")

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
            CREATE OR REPLACE VIEW cream.v_propfit_daily
                AS (
                    SELECT
                        rh.Date,
                        pd.ExchangeTypes,
                        pd.ad_adv,
                        pd.PubId,
                        rh.url,
                        rh.appName,
                        rh.os,
                        rh.osv,
                        rh.req,
                        rh.w AS Width,
                        rh.h AS Height,
                        pd.ad_grp,
                        pd.InventoryIdx,
                        pd.ad_cam,
                        pd.bid,
                        pd.imp,
                        pd.clk,
                        pd.Revenue,
                        b.Name AS CampaignName,
                        b.DailyBudget AS GoalBudget,
                        c.Name AS AdgroupName,
                        d.Name AS AdvertiserName,
                        rh.category01,
                        rh.category02
                    FROM (
                        SELECT
                            CONCAT(year, '-', LPAD(month, 2, 0), '-', LPAD(day, 2, 0)) AS Date, g.requestAppName AS appName, bundle AS url, os, osv, w, h,
                            g.category01, g.category02, SUM(request_count) AS req
                        FROM cream.propfit_request_hourly r
                        LEFT JOIN (SELECT requestAppBundle, requestAppName, category01, category02 FROM ice.google_play_store_info_nhn) g
                            ON r.bundle = g.requestAppBundle
                        WHERE (year = YEAR('{current_time}') AND month IN (MONTH('{criteria_last_date}'), MONTH('{current_time}'), MONTH('{criteria_next_date}')))
                        GROUP BY year, month, day, appName, url, os, osv, w, h, g.category01, g.category02
                        ) rh
                    LEFT JOIN cream.propfit_daily pd
                        ON pd.Date = rh.Date AND pd.url = rh.url AND pd.Width = rh.w AND pd.Height = rh.h AND (pd.Date BETWEEN '{criteria_last_date}' AND '{criteria_next_date}')
                    LEFT JOIN (SELECT CampaignIdx, Name, DailyBudget FROM ice.cm_campaign) b
                        ON pd.ad_cam = b.CampaignIdx
                    LEFT JOIN (SELECT CampaignIdx, AdgroupIdx, Name FROM ice.cm_adgroup) c
                        ON pd.ad_cam = c.CampaignIdx AND pd.ad_grp = c.AdgroupIdx
                    LEFT JOIN (SELECT AdvertiserInfoIdx, Name FROM ice.cm_advertiserinfo) d
                        ON pd.ad_adv = d.AdvertiserInfoIdx
                    )
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-REPLACE-VIEW-PROPFIT-DAILY]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-REPLACE-VIEW-PROPFIT-DAILY]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

