# Databricks notebook source
from pyspark.sql.functions import col,lit
import datetime

# COMMAND ----------

table_name = 'cream.propfit_count_hourly'

# COMMAND ----------

stats_datetime =datetime.datetime.now()+datetime.timedelta(hours=9)-datetime.timedelta(hours=1)

# COMMAND ----------

stats_datetime_year = stats_datetime.strftime("%Y")
stats_datetime_month = stats_datetime.strftime("%m")
stats_datetime_day = stats_datetime.strftime("%d")
stats_datetime_hour = stats_datetime.strftime("%H")
stats_datetime_minute = stats_datetime.strftime("%M")

# COMMAND ----------

# stats_datetime_hour = '20'

# COMMAND ----------

print(stats_datetime_year, stats_datetime_month, stats_datetime_day, stats_datetime_hour, stats_datetime_minute)

# COMMAND ----------

df = spark.read.json(f"dbfs:/mnt/ptbwa-propfit/stats/propfit-count-hourly/year={stats_datetime_year}/month={stats_datetime_month}/day={stats_datetime_day}/hour={stats_datetime_hour}/*")
# df.display()

# COMMAND ----------

df = df.withColumn('year', lit(int(stats_datetime_year))).withColumn('month', lit(int(stats_datetime_month))).withColumn('day', lit(int(stats_datetime_day))).withColumn('hour', lit(int(stats_datetime_hour)))
# df.display()

# COMMAND ----------

delete_query = f"""
delete 
from {table_name}
where year={stats_datetime_year}
    and month={stats_datetime_month}
    and day={stats_datetime_day}
    and hour={stats_datetime_hour}
"""

# print(delete_query)

# COMMAND ----------

spark.sql(delete_query)

# COMMAND ----------

# spark.sql(f"delete from cream.propfit_request_hourly where year={year} and month={month} and day={day} and hour={hour}")

# COMMAND ----------

df.write.mode('append').partitionBy("year", "month", "day", "hour").saveAsTable(table_name)

# COMMAND ----------

