# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### {ADV} Branch Postback Data ETL
# MAGIC - [Branch Postback](https://s3.console.aws.amazon.com/s3/buckets/ptbwa-basic?prefix=propfit-branch/)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Load Packages

# COMMAND ----------

from datetime import datetime, timedelta
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
# MAGIC ##### ETL

# COMMAND ----------

# df_csv = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/ptbwa-basic/propfit-general/com.truefriend.ministock_conversions_2023-03-02_2023-03-06_Asia_Seoul.csv")

# COMMAND ----------

# df_csv = df_csv.withColumnRenamed("Advertising ID", "advertising_id")

# COMMAND ----------

# df_csv = df_csv.withColumnRenamed("Event Time", "event_time")

# COMMAND ----------

# s3://ptbwa-basic/propfit-branch/year=2023/month=03/day=15/hour=12/
df = spark.read.format("parquet").load(f"dbfs:/mnt/ptbwa-basic/propfit-branch/year={current_y}/month={current_m}/day={current_d}/hour=*")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.select("postbackitem.*", "actiontime")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Delete Duplication

# COMMAND ----------

spark.sql(f"delete from ice.af_ministock_postback where date(actiontime) = '{current_y}-{current_m}-{current_d}'")

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