# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Bid Request EDA
# MAGIC - Profile
# MAGIC - Deepdive into Data
# MAGIC - Source
# MAGIC   - `s3://ptbwa-basic/topics/bidrequest_app_nhn/year=*/month=*/day=*/hour=*/minute=*/`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

import pprint

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Data

# COMMAND ----------

# s3://ptbwa-basic/topics/bidrequest_app_nhn/year=2022/month=11/day=22/hour=16/minute=05/

df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/bidrequest_app_nhn/year=2022/month=11/day=22/hour=16/minute=05/")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Spark DF Info

# COMMAND ----------

def check_details(df):
    part_num=0
    print(f"#Partitions = {len(df.rdd.glom().collect())}")
    for p in df.rdd.glom().collect():
        row_num=0
        values=[]
        print(f"----------- P{part_num} -----------")
        for rowInPart in p:
            if(rowInPart[0] not in values): 
                values.append(rowInPart[0])
            row_num=row_num+1
        print(f"  #Rows in P{part_num}  = {row_num}")
        print(f"  Values in P{part_num} = {values if values else 'N/A'}")
        part_num=part_num+1

# COMMAND ----------

check_details(df)

# COMMAND ----------

df = df.repartition(16)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Spark Profile
# MAGIC - General Info

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Specific Analysis
# MAGIC - DeepDive into Each Columns

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

