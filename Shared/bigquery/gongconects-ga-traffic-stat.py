# Databricks notebook source
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("GONGCONECTS")

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
    df = spark.sql(f"DELETE from cream.ga_gongconects_traffic WHERE date = '{criteria_date}'")
    k_log.info(f"[SUCCESS-DELETE-GA-GONGCONECTS-TRAFFIC-STAT]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-DELETE-GA-GONGCONECTS-TRAFFIC-STAT]{e}")
    raise e

# COMMAND ----------

try:
    insert_query = f"""
            INSERT INTO cream.ga_gongconects_traffic
                    SELECT
                        SUBSTRING(event_time, 1, 10) AS date,
                        user_pseudo_id,
                        ga_session_id,
                        tid2 AS tid,
                        MAX(medium) AS medium,
                        MAX(source) AS source,
                        MAX(campaign) AS campaign, 
                        SUM(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS new_user,
                        MIN(TIMESTAMP_MICROS(event_timestamp)) AS min_event_time,
                        MAX(TIMESTAMP_MICROS(event_timestamp)) AS max_event_time,
                        TIMESTAMPDIFF(second, MIN(TIMESTAMP_MICROS(event_timestamp)), MAX(TIMESTAMP_MICROS(event_timestamp))) AS session_duration,
                        SUM(IF(engagement_time_msec IS NULL, 0, engagement_time_msec)) / 1000 AS session_duration_ga,
                        COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS pageview_event,
                        COUNT(CASE WHEN event_name = 'GA4_회원가입완료' THEN 1 END) AS signup_event,
                        COUNT(CASE WHEN event_name = 'GA4_결제완료' THEN 1 END) AS purchase_event,
                        COUNT(event_name) AS event_all_cnt,
                        COUNT(CASE WHEN event_name IN ('GA4_회원가입완료', 'GA4_결제완료') THEN 1 END) AS event_cv_cnt
                    FROM (
                        SELECT *, CASE WHEN tid IS NULL AND page_referrer LIKE '%ad_trx=%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(page_referrer, '&', 2), 'ad_trx=', -1) ELSE tid END AS tid2
                        FROM ice.gongconetcs_daily
                        )
                    WHERE SUBSTRING(event_time, 1, 10) = '{criteria_date}'
                    GROUP BY SUBSTRING(event_time, 1, 10), user_pseudo_id, ga_session_id, tid2
                    ORDER BY SUBSTRING(event_time, 1, 10), user_pseudo_id, ga_session_id, tid2
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-INSERT-GA-GONGCONECTS-TRAFFIC-STAT]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-INSERT-GA-GONGCONECTS-TRAFFIC-STAT]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

