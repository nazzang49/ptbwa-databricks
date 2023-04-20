# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ministock Appsflyer Postback Data ETL
# MAGIC - Re-Engagement Period
# MAGIC   - D-30
# MAGIC   - Update D-30 ~ D-Day Everyday
# MAGIC - [CSV on S3](https://s3.console.aws.amazon.com/s3/object/ptbwa-basic?region=ap-northeast-2&prefix=propfit-general/com.truefriend.ministock_conversions_2023-03-02_2023-03-06_Asia_Seoul.csv)
# MAGIC - [AF Postback](https://s3.console.aws.amazon.com/s3/buckets/ptbwa-basic?region=ap-northeast-2&prefix=propfit-appsflyer/year%3D2023/month%3D03/&showversions=false)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Load Packages

# COMMAND ----------

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

# COMMAND ----------

now = datetime.now() + timedelta(hours=9)
current_y = now.strftime("%Y")
current_m = now.strftime("%m")
current_d = now.strftime("%d")
current_h = now.strftime("%H")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Get Last Month Value

# COMMAND ----------

last_m = now - relativedelta(months=1)
last_m = last_m.strftime("%m")

# COMMAND ----------

last_m

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### ETL

# COMMAND ----------

# df_csv = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/ptbwa-basic/propfit-general/com.truefriend.ministock_conversions_2023-03-02_2023-03-06_Asia_Seoul.csv")

# COMMAND ----------

# df_csv = df_csv.withColumnRenamed("Advertising ID", "advertising_id")

# COMMAND ----------

# df_csv = df_csv.withColumnRenamed("Event Time", "event_time")

# COMMAND ----------

df_last = spark.read.format("parquet").load(f"dbfs:/mnt/ptbwa-basic/propfit-appsflyer/year={current_y}/month={last_m}/day=*/hour=*")

# COMMAND ----------

df_current = spark.read.format("parquet").load(f"dbfs:/mnt/ptbwa-basic/propfit-appsflyer/year={current_y}/month={current_m}/day=*/hour=*")

# COMMAND ----------

df_current.show(5)

# COMMAND ----------

df = df_last.union(df_current)

# COMMAND ----------

df = df.select("postbackitem.*", "actiontime")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Delete Duplication

# COMMAND ----------

spark.sql(f"delete from ice.af_ministock_postback where date(actiontime) >= '{current_y}-{last_m}-01'")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Save

# COMMAND ----------

df.write.mode("append").format("delta").saveAsTable("ice.af_ministock_postback")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Comparison
# MAGIC - CSV vs Postback

# COMMAND ----------

# df_csv.createOrReplaceTempView("csv")

# COMMAND ----------

# df_af.createOrReplaceTempView("af")

# COMMAND ----------

# %sql

# select count(*) from csv
# where event_time >= '2023-03-06'

# COMMAND ----------

# %sql

# select distinct bundle_id
# from af

# COMMAND ----------

# %sql

# select af.aos_device_id
# from af
# join csv
# where af.aos_device_id = csv.advertising_id

# COMMAND ----------

# 3월 3일 16시부터 데이터 정상 유입
# s3://ptbwa-basic/propfit-appsflyer/year=2023/month=03/day=03/hour=13/Appsflyer-KDF-34-2023-03-03-04-50-17-079a7dd2-3483-380c-8ab8-c9108a2a0357.parquet

# COMMAND ----------

# spark.read.format("parquet").load("dbfs:/mnt/ptbwa-basic/propfit-appsflyer/year=2023/month=03/day=03/hour=13/Appsflyer-KDF-34-2023-03-03-04-50-17-079a7dd2-3483-380c-8ab8-c9108a2a0357.parquet").display()