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
    df = spark.sql(f"DELETE from cream.ga_migun_traffic WHERE date = '{criteria_date}'")
    k_log.info(f"[SUCCESS-DELETE-GA-MIGUN-TRAFFIC-STAT]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-DELETE-GA-MIGUN-TRAFFIC-STAT]{e}")
    raise e

# COMMAND ----------

try:
    insert_query = f"""
            INSERT INTO cream.ga_migun_traffic
                    SELECT
                        SUBSTRING(event_time, 1, 10) AS date,
                        user_pseudo_id,
                        ga_session_id,
                        tid2 AS tid,
                        MAX(medium) AS medium,
                        MAX(source) AS source,
                        MAX(campaign) AS campaign, 
                        SUM(CASE WHEN event_name = 'first_visit' THEN 1 ELSE 0 END) AS new_user,
                        MIN(event_time) AS min_event_time,
                        MAX(event_time) AS max_event_time,
                        TIMESTAMPDIFF(second, MIN(event_time), MAX(event_time)) AS session_duration,
                        SUM(IF(engagement_time_msec IS NULL, 0, engagement_time_msec)) / 1000 AS session_duration_ga,
                        COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS pageview_event,
                        COUNT(CASE WHEN event_name = '미소모집_Click_상단_상담 신청하기' THEN 1 END) AS requestUpper_event,
                        COUNT(CASE WHEN event_name = '미소모집_Click_중간_상담 신청하기' THEN 1 END) AS requestMiddle_event,
                        COUNT(CASE WHEN event_name = '미소모집_하단_상담 신청완료' THEN 1 END) AS requestLower_event,
                        COUNT(CASE WHEN event_name = '미소모집_팝업_Click_상담 신청완료' THEN 1 END) AS requestPopup_event,
                        COUNT(event_name) AS event_all_cnt,
                        COUNT(CASE WHEN event_name IN ('미소모집_Click_상단_상담 신청하기', '미소모집_Click_중간_상담 신청하기',
                                                       '미소모집_하단_상담 신청완료', '미소모집_팝업_Click_상담 신청완료') THEN 1 END) AS event_cv_cnt

                    FROM (
                        SELECT *, CASE WHEN tid IS NULL AND page_referrer LIKE '%ad_trx=%' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(page_referrer, '&', 2), 'ad_trx=', -1) ELSE tid END AS tid2
                        FROM ice.migun_daily
                        )
                    WHERE SUBSTRING(event_time, 1, 10) = '{criteria_date}'
                    GROUP BY SUBSTRING(event_time, 1, 10), user_pseudo_id, ga_session_id, tid2
                    ORDER BY SUBSTRING(event_time, 1, 10), user_pseudo_id, ga_session_id, tid2
        """            
    df = spark.sql(insert_query)
    k_log.info(f"[SUCCESS-INSERT-GA-MIGUN-TRAFFIC-STAT]{df.collect()}")
except Exception as e:
    print(e)
    k_log.error(f"[FAIL-INSERT-GA-MIGUN-TRAFFIC-STAT]{e}")
    raise e

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

