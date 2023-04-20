# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Connection with TG360 Data
# MAGIC - [S3](https://s3.console.aws.amazon.com/s3/buckets/ptbwa-tg?region=ap-northeast-2&tab=objects)
# MAGIC - Code

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Sample Data
# MAGIC - S3

# COMMAND ----------

# s3://ptbwa-tg/20230313/

# COMMAND ----------

# df = spark.read.parquet("dbfs:/mnt/ptbwa-tg/20230313/")

# COMMAND ----------

# df.display()

# COMMAND ----------

# (df
#  .write
#  .format("delta")
#  .mode("append")
#  .saveAsTable("ice.propfit_tg"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Code File
# MAGIC - CSV

# COMMAND ----------

# df = spark.read.option("header", "true").csv("dbfs:/FileStore/tg_code_total.csv")

# COMMAND ----------

# df = (df
#  .withColumnRenamed("카테고리 코드", "category_code")
#  .withColumnRenamed("속성 코드", "attribution_code")
#  .withColumnRenamed("카테고리", "category")
#  .withColumnRenamed("속성 이름", "attribution"))

# COMMAND ----------

# (df
#  .write
#  .format("delta")
#  .mode("append")
#  .saveAsTable("ice.propfit_tg_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### EDA

# COMMAND ----------

df = spark.sql("select * from hive_metastore.ice.propfit_tg")

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------



# COMMAND ----------

