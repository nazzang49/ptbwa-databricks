# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Propfit Stat Compare with @DEV - @DATA
# MAGIC - `2022-01-09` V1
# MAGIC - 개발팀 - 데이터팀 성과 수치 비교 데이터 연동
# MAGIC - Path
# MAGIC   - S3
# MAGIC       - `s3://ptbwa-basic/propfit-stat/{daily}.csv`
# MAGIC   - Delta
# MAGIC       - `cream.propfit_stat_compare`
# MAGIC - Job Scheduling
# MAGIC   - @Daily at 10:30AM
# MAGIC - QnA to @jinyoung.park

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing

# COMMAND ----------

current = datetime.now()
current_date = current.strftime("%Y%m%d")

# COMMAND ----------

current_date

# COMMAND ----------

# s3://ptbwa-basic/propfit-stat/20230109.csv

# COMMAND ----------

df = spark.read.option("header", "true").csv(f"dbfs:/mnt/ptbwa-basic/propfit-stat/{current_date}.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Select Cols

# COMMAND ----------

df = df.drop("_id")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Delete and Save

# COMMAND ----------

criteria_date = current - timedelta(days=1)
criteria_date = criteria_date.strftime("%Y-%m-%d")

# COMMAND ----------

criteria_date

# COMMAND ----------

spark.sql(f"delete from cream.propfit_stat_compare where date_format(Date, 'yyyy-MM-dd') = '{criteria_date}'")

# COMMAND ----------

df.write.mode("append").format("delta").saveAsTable("cream.propfit_stat_compare")