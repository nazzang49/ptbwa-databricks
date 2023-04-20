# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Schema Diff
# MAGIC - Use Case
# MAGIC   - Diff on Databases PROD vs DEV
# MAGIC   - e.g. `auto_report` vs `tt_auto_report`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
s_log = log4jLogger.LogManager.getLogger("SCHEMA-DIFF")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

!python -m pip install chispa

# COMMAND ----------

from chispa.schema_comparer import assert_schema_equality, SchemasNotEqualError
from databricks_mgmt import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - Required
# MAGIC   - database
# MAGIC - Optional
# MAGIC   - tables

# COMMAND ----------

try:
    # (!) LIVE
    database = dbutils.widgets.get("database")
    
    # (!) TEST
#     database = "auto_report"
except Exception as e:
    s_log.error("[FAIL-GET-DATABASE-PARAMS]REQUIRED")
    raise e

# COMMAND ----------

# try:
#     # (!) LIVE
#     database = dbutils.widgets.get("database")
    
#     # (!) TEST
#     tables = ["gsh_kcar_monthly_kpi"]
# except Exception as e:
#     log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Schema Function

# COMMAND ----------

def is_same_schema(dev_tables, prod_tables):
    prod_database = prod_tables[0]["database"]
    mismatched_tables = []
    for dev_table in dev_tables:
        try:
            dev_df = spark.sql(f"select * from {dev_table['database']}.{dev_table['tableName']}")
            prod_df = spark.sql(f"select * from {prod_database}.{dev_table['tableName']}")
            assert_schema_equality(dev_df.schema, prod_df.schema)
        except Exception as e:
            if isinstance(e, SchemasNotEqualError):
                s_log.info(f"[SCHEMA-NOT-SAME]TABLE::{dev_table}")
                print(f"[SCHEMA-NOT-SAME]TABLE::{dev_table['tableName']}")
                print(e)
                mismatched_tables.append(dev_table)
            else:
                s_log.info(f"[UNEXPECTED-EXCEPTION]TABLE::{dev_table}")
                print(f"[UNEXPECTED-EXCEPTION]TABLE::{dev_table['tableName']}")
                print(e)
    return mismatched_tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing
# MAGIC 1. Diff & Extract Mismatched Tables
# MAGIC 2. Drop Previous Tables
# MAGIC 3. Clone Latest-Version Tables
# MAGIC 4. Diff Again

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Diff

# COMMAND ----------

dev_tables = spark.sql(f"show tables in tt_{database}").collect()

# COMMAND ----------

dev_tables

# COMMAND ----------

prod_tables = spark.sql(f"show tables in {database}").collect()

# COMMAND ----------

prod_tables

# COMMAND ----------

mismatched_tables = is_same_schema(dev_tables, prod_tables)

# COMMAND ----------

mismatched_tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Drop

# COMMAND ----------

for mismatched_table in mismatched_tables:
    spark.sql(f"drop table if exists {mismatched_table['database']}.{mismatched_table['tableName']}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Clone

# COMMAND ----------

for mismatched_table in mismatched_tables:
    m_database = mismatched_table['database']
    m_table_name = mismatched_table['tableName']
    spark.sql(f"create or replace table {m_database}.{m_table_name} clone {database.replace('tt_', '')}.{m_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Diff Again

# COMMAND ----------

dev_tables = spark.sql(f"show tables in tt_{database}").collect()
prod_tables = spark.sql(f"show tables in {database}").collect()

# COMMAND ----------

mismatched_tables = is_same_schema(dev_tables, prod_tables)

# COMMAND ----------

mismatched_tables