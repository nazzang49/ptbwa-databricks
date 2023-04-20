# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ### Gongconetcs Daily Report
# MAGIC - 공단기

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

import json
import base64

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Config

# COMMAND ----------

with open("/dbfs/FileStore/configs/bq_ga4_gongconetcs_config.json", "r") as f:
    config = json.load(f)

# COMMAND ----------

config

# COMMAND ----------

encoded_config = json.dumps(config).encode('utf-8')
credentials = base64.b64encode(encoded_config)
credentials = credentials.decode('utf-8')

# COMMAND ----------

credentials

# COMMAND ----------

project = "gongconetcs"
parent_project = "bq-ga4-gongconetcs"
report_date = datetime.strftime((datetime.now() - timedelta(1)), "%Y%m%d")
r_dataset = "analytics_254229969"
# w_dataset = "vincent"

# COMMAND ----------

if dbutils.widgets.getArgument("report_date", ""):
    report_date = dbutils.widgets.get("report_date")

# COMMAND ----------

try:
    query_option1 = f"""
    SELECT
        aa.event_date, aa.event_time, aa.event_name, aa.event_timestamp,
        aa.user_pseudo_id, aa.user_id, aa.stream_id, aa.platform,
        max(prod) prod,
        max(ga_session_id) ga_session_id,
        max(ga_session_number) ga_session_number,
        max(page_location) page_location,
        max(page_title) page_title,
        max(page_referrer) page_referrer,
        max(campaign) campaign,
        max(medium) medium,
        max(source) source,
        max(term) term,
        max(content) content,
        max(engagement_time_msec) engagement_time_msec,
        aa.user_first_touch_timestamp,
        aa.category, aa.region,
        aa.city, aa.t_name,
        aa.t_medium, aa.t_source,
    FROM (
        SELECT
            a.event_date, DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_time, a.event_name, a.event_timestamp,
            a.user_pseudo_id, a.user_id, a.stream_id, a.platform,
            if(b.key='prod', b.value.string_value, null) as prod,
            if(b.key='ga_session_id', b.value.int_value, null) as ga_session_id,
            if(b.key='ga_session_number', b.value.int_value, null) as ga_session_number,
            if(b.key='page_location', b.value.string_value, null) as page_location,
            if(b.key='page_title', b.value.string_value, null) as page_title,
            if(b.key='page_referrer', b.value.string_value, null) as page_referrer,
            if(b.key='campaign', b.value.string_value, null) as campaign,
            if(b.key='medium', b.value.string_value, null) as medium,
            if(b.key='source', b.value.string_value, null) as source,
            if(b.key='term', b.value.string_value, null) as term,
            if(b.key='content', b.value.string_value, null) as content,
            if(b.key='engagement_time_msec', b.value.int_value, null) as engagement_time_msec,
            a.user_first_touch_timestamp,
            a.device.category,
            a.geo.region,
            a.geo.city,
            a.traffic_source.name as t_name,
            a.traffic_source.medium as t_medium,
            a.traffic_source.source as t_source,
        FROM
            `{parent_project}.{r_dataset}.events_{report_date}` a CROSS JOIN unnest (event_params) as b
        WHERE 
            a.event_name NOT IN ('scroll')
    ) aa
    GROUP BY
        aa.event_date, aa.event_time, aa.event_name, aa.event_timestamp,
        aa.user_pseudo_id, aa.user_id, aa.stream_id, aa.platform,
        aa.user_first_touch_timestamp,
        aa.category,
        aa.region,
        aa.city,
        aa.t_name,
        aa.t_medium,
        aa.t_source
    """
except Exception as e:
    log.error("[FAIL-CREATE-QUERY-OPTION-1]")
    raise Exception(e)

# COMMAND ----------

try:
    query_option2 = f"""
    SELECT
        aa.event_date, aa.event_time, aa.event_name, aa.event_timestamp,
        aa.user_pseudo_id, aa.user_id, aa.stream_id, aa.platform,
        max(prod) prod,
        max(ga_session_id) ga_session_id,
        max(ga_session_number) ga_session_number,
        max(page_location) page_location,
        max(page_title) page_title,
        max(page_referrer) page_referrer,
        max(campaign) campaign,
        max(medium) medium,
        max(source) source,
        max(term) term,
        max(content) content,
        max(engagement_time_msec) engagement_time_msec,
        aa.user_first_touch_timestamp,
        aa.category, aa.region,
        aa.city, aa.t_name,
        aa.t_medium, aa.t_source,
    FROM (
        SELECT
            a.event_date, DATETIME(TIMESTAMP_MICROS(event_timestamp), 'Asia/Seoul') as event_time, a.event_name, a.event_timestamp,
            a.user_pseudo_id, a.user_id, a.stream_id, a.platform,
            if(b.key='prod', b.value.string_value, null) as prod,
            if(b.key='ga_session_id', b.value.int_value, null) as ga_session_id,
            if(b.key='ga_session_number', b.value.int_value, null) as ga_session_number,
            if(b.key='page_location', b.value.string_value, null) as page_location,
            if(b.key='page_title', b.value.string_value, null) as page_title,
            if(b.key='page_referrer', b.value.string_value, null) as page_referrer,
            if(b.key='campaign', b.value.string_value, null) as campaign,
            if(b.key='medium', b.value.string_value, null) as medium,
            if(b.key='source', b.value.string_value, null) as source,
            if(b.key='term', b.value.string_value, null) as term,
            if(b.key='content', b.value.string_value, null) as content,
            if(b.key='engagement_time_msec', b.value.int_value, null) as engagement_time_msec,
            a.user_first_touch_timestamp,
            a.device.category,
            a.geo.region,
            a.geo.city,
            a.traffic_source.name as t_name,
            a.traffic_source.medium as t_medium,
            a.traffic_source.source as t_source,
        FROM
            `{parent_project}.{r_dataset}.events_intraday_{report_date}` a CROSS JOIN unnest (event_params) as b
        WHERE 
            a.event_name NOT IN ('scroll')
    ) aa
    GROUP BY
        aa.event_date, aa.event_time, aa.event_name, aa.event_timestamp,
        aa.user_pseudo_id, aa.user_id, aa.stream_id, aa.platform,
        aa.user_first_touch_timestamp,
        aa.category,
        aa.region,
        aa.city,
        aa.t_name,
        aa.t_medium,
        aa.t_source
    """
except Exception as e:
    log.error("[FAIL-CREATE-QUERY-1]")
    raise Exception(e)

# COMMAND ----------

args = {
    "project": project,
    "parent_project": parent_project,
    "report_date": report_date,
    "r_table_path_option1": f"{parent_project}.{r_dataset}.events_{report_date}",
    "r_table_path_option2": f"{parent_project}.{r_dataset}.events_intraday_{report_date}",
    "r_format": "bigquery",
    "r_dataset": r_dataset,
#     "w_dataset": w_dataset,
#     "w_format": "bigquery",
#     "w_table_path": f"{parent_project}.{w_dataset}.welrix_daily_{report_date}",
#     "w_mode": "overwrite",
    "gcs_bucket_name": "databricks-dev-bq",
    "s3_upload_path": f"/mnt/ptbwa-basic/{project}-daily/{report_date}",
    "query_option1": query_option1,
    "query_option2": query_option2,
    "delta_table_path": f"ice.{project}_daily",
    "private_key": config["private_key"],
    "private_key_id": config["private_key_id"],
    "client_email": config["client_email"],
    "credentials": credentials,
}

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Run Another Notebook
# MAGIC - Trigger Common Daily

# COMMAND ----------

dbutils.notebook.run("/Users/jinyoung.park@ptbwa.com/common-daily-report", timeout_seconds=1500, arguments=args)