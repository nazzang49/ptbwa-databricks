# Databricks notebook source
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("MIGUN")

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone

# COMMAND ----------

KST = timezone('Asia/Seoul')
criteria_date = datetime.strftime((datetime.now(KST) - timedelta(1)), "%Y-%m-%d")
criteria_date

# COMMAND ----------

current_date = datetime.strftime((datetime.now(KST)), "%Y-%m-%d")
current_date

# COMMAND ----------

try:
    df = spark.sql(f"DELETE from cream.propfit_ga_migun_daily WHERE date = '{criteria_date}'")
    k_log.info(f"[SUCCESS-DELETE-PROPFIT-GA-MIGUN-DAILY]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-DELETE-PROPFIT-GA-MIGUN-DAILY]{e}")
    raise e

# COMMAND ----------

try:
    insert_query = f"""
            INSERT INTO cream.propfit_ga_migun_daily
                    SELECT DISTINCT
                        jd.*,
                        gt.user_pseudo_id,
                        gt.ga_session_id,
                        gt.medium,
                        gt.source,
                        gt.campaign,
                        gt.new_user AS newUser,
                        gt.min_event_time,
                        gt.max_event_time,
                        gt.session_duration,
                        gt.session_duration_ga,
                        gt.pageview_event,
                        gt.requestUpper_event,
                            gt.requestMiddle_event,
                            gt.requestLower_event,
                            gt.requestPopup_event,
                        gt.event_all_cnt,
                        gt.event_cv_cnt
                    FROM cream.propfit_daily_wobid_raw jd
                    LEFT JOIN (
                        SELECT *
                        FROM cream.ga_migun_traffic
                        WHERE user_pseudo_id IN (SELECT DISTINCT user_pseudo_id FROM cream.ga_migun_traffic
                                                 WHERE tid IN (SELECT DISTINCT tid FROM ice.streams_imp_bronze_app_nhn WHERE DATE(actiontime_local) = '{criteria_date}'))
                        AND min_event_time BETWEEN TIMESTAMP(CONCAT('{criteria_date}', ' 00:00:00')) and TIMESTAMP(CONCAT('{current_date}', ' 02:00:00'))
                        ) gt
                        ON jd.tid = gt.tid
                    WHERE jd.clktime IS NOT NULL
                    AND jd.Date = '{criteria_date}'
                    AND jd.ad_adv = '5'
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-INSERT-PROPFIT-GA-MIGUN-DAILY]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-INSERT-PROPFIT-GA-MIGUN-DAILY]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

