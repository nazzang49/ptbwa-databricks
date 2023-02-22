# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Check Data Validation
# MAGIC - Check List
# MAGIC   - Total Count
# MAGIC   - Date Min, Max
# MAGIC   - Distinct Count
# MAGIC   - Yesterday Count
# MAGIC   - Null Exist Column
# MAGIC   - (Optional) Randomly 1-row Validation
# MAGIC - Arguments
# MAGIC   - Schema Name
# MAGIC   - Table Name
# MAGIC   - Period
# MAGIC     - Timestamp
# MAGIC     - Datetime

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
v_log = log4jLogger.LogManager.getLogger("DATA-VALIDATION")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

import pprint
import json

from datetime import datetime, timedelta
from enum import Enum
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Utils
# MAGIC - Check Result
# MAGIC - Enum Types

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Get Params
# MAGIC - If Washing Process

# COMMAND ----------

s_date = None
e_date = None
env = None
project = None
table_names = None

# COMMAND ----------

try:
    s_date = dbutils.widgets.get("s_date")
    
#     s_date = '2022-11-01'
except Exception as e:
    v_log.info("[NOT-FOUND-WASHING-PERIOD]MANUAL-VALIDATION-PROCESS")

# COMMAND ----------

try:
    e_date = dbutils.widgets.get("e_date")

#     e_date = '2022-12-01'
except Exception as e:
    v_log.info("[NOT-FOUND-WASHING-PERIOD]MANUAL-VALIDATION-PROCESS")

# COMMAND ----------

try:
    table_names = dbutils.widgets.get("table_names")
    
    # (!) test
#     table_name = "tt_kmt_kcar_ad_wash_stats"
except Exception as e:
    v_log.info("[NOT-FOUND-TABLE-NAMES]MANUAL-VALIDATION-PROCESS")

# COMMAND ----------

try:
    env = dbutils.widgets.get("env")
    
    # (!) test
#     env = "dev"
except Exception as e:
    v_log.info("[NOT-FOUND-ENV]PROD(DEFAULT)")

# COMMAND ----------

try:
    project = dbutils.widgets.get("project")
    
    # (!) test
#     env = "dev"
except Exception as e:
    v_log.info("[NOT-FOUND-PROJECT]AUTOREPORT(DEFAULT)")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Utils

# COMMAND ----------

class PtbwaUtils:
    """
    A class for dealing with repeated functions
    """
    @staticmethod
    def check_result(domain, result):
        try:
            print(f"[{domain}-RESULT]")
            pprint.pprint(result)
            print()
            v_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

class AutoReportUtils:
    """
    A class for dealing with repeated functions
    """
    @staticmethod
    def create_filter_query_by_date(date_col: str, s_date: str, e_date: str):
        if date_col is None:
            raise ValueError("[INVALID-VALUE]REQUIRED::{date_col}")
        
        if s_date is None or e_date is None:
            raise ValueError("[INVALID-VALUE]REQUIRED::{s_date, e_date}")
        return f"{date_col} between '{s_date}' and '{e_date}'"

# COMMAND ----------

class PtbwaDataTypes(Enum):
    """
    A enum class for inserting arguments as types
    """
    DATABASE_ICE = "ice"
    DATABASE_CREAM = "cream"
    DATABASE_DEFAULT = "default"
    DATABASE_AUTOREPORT = "auto_report"
    DATABASE_AUTOREPORT_TEST = "tt_auto_report"

    TABLE_NAME_TYPE_FULL = "full"
    TABLE_NAME_TYPE_REGEX = "regex"
    TABLE_NAME_TYPE_CONTAINS = "contains"    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Validator
# MAGIC - Processing Data Validation
# MAGIC - Save Result to Delta

# COMMAND ----------

class PtbwaDataValidator:
    """
    A processor class for validating PTBWA Delta Tables
    """
    
    _valid_databases = [
        "ice",
        "cream"
        "auto_report",
        "tt_auto_report",
        "default"
    ]
    
    _valid_table_name_types = [
        "full",
        "contains",
        "regex"
    ]
    
    def __init__(self, database, table_name_type, table_names, **kwargs):
        self.database = database
        self.table_name_type = table_name_type
        self.table_names = table_names
        
        if ("start_date" in kwargs and "end_date" not in kwargs) or ("end_date" in kwargs and "start_date" not in kwargs):
            raise ValueError("[INVALID-VALUE]REQUIRED::{start_date, end_date}")
            
        if "start_date" in kwargs and "end_date" in kwargs:
            self.start_date = kwargs["start_date"]
            self.end_date = kwargs["end_date"]
            
    @property
    def database(self):
        """
        A property getting database
        """
        return self._database
    
    @database.setter
    def database(self, database):
        """
        A property setting database
        """
#         assert database in self._valid_databases, "[INVALID-DATABASE]NOT-FOUND-DATABASE"
        self._database = database
        
    @property
    def table_name_type(self):
        """
        A property getting table_name_type
        """
        return self._table_name_type
    
    @table_name_type.setter
    def table_name_type(self, table_name_type):
        """
        A property setting table_name_type
        """
        assert table_name_type in self._valid_table_name_types, "[INVALID-TABLE-NAME-TYPE]NOT-FOUND-TABLE-NAME-TYPE"
        self._table_name_type = table_name_type
    
    def set_up(self):
        """
        A method for processing all methods
        """
        self.get_tables()
        self.search_tables()
    
    def get_tables(self):
        """
        A method for getting tables from specific database
        """
        try:
            self.tables = spark.sql(f"show tables in {self.database}").collect()
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def search_tables(self):
        """
        A method for searching specific tables in {self.tables}
        """
        try:
            if self.table_name_type == "full":
                # (!) prod
#                 self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name == table["tableName"] and "tt_" not in table["tableName"]]

                # (!) dev
                self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name == table["tableName"]]
            elif self.table_name_type == "contains":
                self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name in table["tableName"] and "tt_" not in table["tableName"]]
            elif self.table_name_type == "regex":
                pass
            else:
                raise ValueError("[INVALID-TABLE-NAME-TYPE]NOT-FOUND-TABLE-NAME-TYPE")
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

class PtbwaDataValidationProcessor:
    """
    A class for validating PTBWA Delta Tables
    """
    
    def __init__(self, ptbwa_data_validator:PtbwaDataValidator):
        self.json_rows = []
        self.date_cols = []
        self.ptbwa_data_validator = ptbwa_data_validator
        PtbwaUtils.check_result(domain="PTBWA-DATA-VALIDATOR", result=self.ptbwa_data_validator.__dict__)
        
        self.now = datetime.now() + timedelta(hours=9)
        self.now_str = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d %H:%M:%S")
        
        self.yesterday = self.now - timedelta(days=1)
        self.yesterday_str = datetime.strftime(self.yesterday, "%Y-%m-%d")
        
        # (!) ========================================== required {is_washing} param from airflow washing
        self.is_washing = False
        if self.ptbwa_data_validator.start_date and self.ptbwa_data_validator.end_date and self.is_washing:
            self.ptbwa_data_validator.database = PtbwaDataTypes.DATABASE_AUTOREPORT.value
            self.ptbwa_data_validator.table_name_type = PtbwaDataTypes.TABLE_NAME_TYPE_FULL.value
            
    def check_tables(self):
        """
        A method for checking each tables orderly
        """
        try:
            for table in self.ptbwa_data_validator.searched_tables:
                self.read_table(table)
                
                min_date, max_date = self.check_date(table)
                period = f"{self.ptbwa_data_validator.start_date}|{self.ptbwa_data_validator.end_date}" if self.is_washing else f"{self.yesterday_str}|{self.yesterday_str}"
                self.json_rows.append(
                    json.dumps({
                        "database": self.ptbwa_data_validator.database,
                        "table": table,
                        "total_count": self.check_total_count(table),
                        "distinct_count": self.check_distinct_count(table),
                        "period_count": self.check_period_count(table),
                        "min_date": min_date,
                        "max_date": max_date,
                        "null_exist_cols": self.check_null_exist_cols(table),
                        "is_washing": self.is_washing,
                        "period": period,
                        "validated_at": self.now_str,
                        "validated_by": "데이터솔루션팀_박진영"
                }))
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
    
    def read_table(self, table):
        """
        A method for reading data in searched tables
        (!) caution:
            check date format before running e.g. %Y-%m-%d
        """
        try:
            query = f"SELECT * FROM {self.ptbwa_data_validator.database}.{table}"
            ptbwa_data_validator_args = self.ptbwa_data_validator.__dict__
            df = spark.sql(query)
            
            # (!) retrieve candidates of date-cols e.g. "start" in kakaopay-related tables
            date_cols = [col for col in df.columns if col.startswith(("date", "Date", "start")) or col.endswith(("date", "Date", "start"))]
            
            # (!) check types e.g. string or date or timestamp
            self.date_cols.clear()
            for col in date_cols:
                if isinstance(df.schema[col].dataType, (DateType, StringType, TimestampType)):
                    self.date_cols.append(col)
                    PtbwaUtils.check_result(f"IS-DATE-COL|TABLE::{table}", col)
                    break
                else:
                    PtbwaUtils.check_result(f"NOT-DATE-COL|TABLE::{table}", col)
            
            if self.date_cols and "start_date" in ptbwa_data_validator_args and "end_date" in ptbwa_data_validator_args:
                PtbwaUtils.check_result(
                    domain="DATE", 
                    result=f"TABLE::{table}/COL::{self.date_cols[0]}/START::{self.ptbwa_data_validator.start_date}/END::{self.ptbwa_data_validator.end_date}"
                )
                query += f" WHERE {self.date_cols[0]} >= '{self.ptbwa_data_validator.start_date}' AND {self.date_cols[0]} <= '{self.ptbwa_data_validator.end_date}'"

            setattr(self, f"{table}_df", spark.sql(query))
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
    
    def check_total_count(self, table):
        """
        A method for checking total count
        """
        try:
            return getattr(self, f"{table}_df").count()
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
            
    def check_distinct_count(self, table):
        """
        A method for checking duplicate count
        """
        try:
            return getattr(self, f"{table}_df").distinct().count()
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
    
    def check_period_count(self, table):
        """
        A method for checking duplicate count
        """
        try:
            if not self.date_cols:
                PtbwaUtils.check_result(f"{table.upper()}-NOT-FOUND-DATE-COLS", f"COUNT::0")
                return 0
        
            filter_query = AutoReportUtils.create_filter_query_by_date(
                self.date_cols[0], 
                self.yesterday_str,
                self.yesterday_str
            )
            
            if self.is_washing:
                filter_query = AutoReportUtils.create_filter_query_by_date(
                    self.date_cols[0], 
                    self.ptbwa_data_validator.start_date,
                    self.ptbwa_data_validator.end_date
                )
            
            return getattr(self, f"{table}_df").filter(filter_query).count()
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
    
    def check_date(self, table):
        """
        A method for checking date e.g. min, max
        capable types:
            int e.g. tv_gad_kakaopay_advideo_stats_d.DateInt
            date
            string
        """
        try:
            if not self.date_cols:
                return None, None
            
            df = getattr(self, f"{table}_df")
            dates = df.agg(min(self.date_cols[0]).alias("min_date"), max(self.date_cols[0]).alias("max_date")).collect()[0]
            
            if self.is_washing:
                filter_query = AutoReportUtils.create_filter_query_by_date(
                    self.date_cols[0], 
                    self.ptbwa_data_validator.start_date,
                    self.ptbwa_data_validator.end_date
                )
                
                dates = df.filter(filter_query).agg(min(self.date_cols[0]).alias("min_date"), max(self.date_cols[0]).alias("max_date")).collect()[0]
            
            return dates["min_date"], dates["max_date"]
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
            
    def check_null_exist_cols(self, table):
        """
        A method for checking null exist cols
        """
        try:
            invalid_types = ("boolean", "struct", "date")
            df = getattr(self, f"{table}_df")
            return df.select([count(when(col(c).isNull(), c)).alias(c) for (c, c_type) in df.dtypes if c_type not in invalid_types and "." not in c]).toJSON().first()
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
            
    def save_to_delta(self):
        """
        A method for saving to delta {default} database
        """
        try:
            json_rdd = sc.parallelize(self.json_rows)
            (spark
                 .read
                 .json(json_rdd)
                 .write
                 .format("delta")
                 .mode("append")
                 .saveAsTable("data_validation_result"))
        except Exception as e:
            v_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing
# MAGIC - Capable of Setting Period
# MAGIC   - `start_date="2022-10-01"`
# MAGIC   - `end_date="2022-10-02"`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### AutoReport Daily / Washing
# MAGIC - Required When Washing
# MAGIC   - s_date
# MAGIC   - e_date

# COMMAND ----------

# (!) table
default_table_names = ["gad"]
if table_names:
    default_table_names = table_names.split(",")

# COMMAND ----------

default_table_names

# COMMAND ----------

# (!) database
database = getattr(PtbwaDataTypes, f"DATABASE_{project}").value

# COMMAND ----------

database

# COMMAND ----------

ptbwa_data_validator = PtbwaDataValidator(
    database, 
    PtbwaDataTypes.TABLE_NAME_TYPE_FULL.value, 
    default_table_names,
    start_date=s_date,
    end_date=e_date
)
ptbwa_data_validator.set_up()

# COMMAND ----------

ptbwa_data_validator.searched_tables

# COMMAND ----------

ptbwa_data_validation_processor = PtbwaDataValidationProcessor(ptbwa_data_validator)

# COMMAND ----------

ptbwa_data_validation_processor.check_tables()

# COMMAND ----------

ptbwa_data_validation_processor.json_rows

# COMMAND ----------

ptbwa_data_validation_processor.save_to_delta()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Spark Dataframe Profile
# MAGIC - Quick Insights of Each Columns

# COMMAND ----------

# dbutils.data.summarize(df_tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

# ptbwa_data_validation_processor.json_rows

# COMMAND ----------

# json_rdd = sc.parallelize(ptbwa_data_validation_processor.json_rows)
# df = spark.read.json(json_rdd)

# COMMAND ----------

# df.display()

# COMMAND ----------

# "tables" in ptbwa_data_validation_processor.ptbwa_data_validator.__dict__

# COMMAND ----------

# ptbwa_data_validator.searched_tables

# COMMAND ----------

# spark.sql("select * from ice.ad_series_cohort_all").agg(min("Date").alias("min_date"), max("Date").alias("max_date")).collect()

# COMMAND ----------

# df_tmp = spark.sql("select * from auto_report.ad_series_cohort_stats")
# df.select(df.colRegex("`.*Date.*|.*roup.*`")).first()

# COMMAND ----------

# tmp = df.select([count(when(isnan(c) | expr(f"{c} is null"), c)).alias(c) for c in df.columns])

# COMMAND ----------

# [table["tableName"] for table_name in ptbwa_data_validator.table_names for table in ptbwa_data_validator.tables if table_name in table["tableName"]]

# COMMAND ----------

# df_tmp.dtypes

# COMMAND ----------

# import pprint

# # offsets 9
# d1 = {"batchWatermarkMs":0,"batchTimestampMs":1667543404575,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
# {"seqNum":234,"sourceVersion":1,"lastBackfillStartTimeMs":1667453455118,"lastBackfillFinishTimeMs":1667453456546}

# pprint.pprint(d1)

# COMMAND ----------

# # offsets 10
# d2 = {"batchWatermarkMs":0,"batchTimestampMs":1667543104443,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
# {"seqNum":228,"sourceVersion":1,"lastBackfillStartTimeMs":1667453455118,"lastBackfillFinishTimeMs":1667453456546}

# pprint.pprint(d2)

# COMMAND ----------

# # offsets 1
# d3 = {"batchWatermarkMs":0,"batchTimestampMs":1667454052617,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider","spark.sql.streaming.join.stateFormatVersion":"2","spark.sql.streaming.stateStore.compression.codec":"lz4","spark.sql.streaming.stateStore.rocksdb.formatVersion":"5","spark.sql.streaming.statefulOperator.useStrictDistribution":"true","spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion":"2","spark.sql.streaming.multipleWatermarkPolicy":"min","spark.sql.streaming.aggregation.stateFormatVersion":"2","spark.sql.shuffle.partitions":"200"}}
# {"seqNum":202,"sourceVersion":1,"lastBackfillStartTimeMs":1667453455118,"lastBackfillFinishTimeMs":1667453456546}

# pprint.pprint(d3)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Confirm CheckpointLocation on `streams_*` Tables after Init File Loading
# MAGIC - `2022-11-21 15:30`
# MAGIC - by DLT

# COMMAND ----------

# !cat /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/checkpoints/streams_bid_bronze_app_nhn/0/sources/0/rocksdb/logs/001011-67496d50-78f0-4bc7-859b-328dab98b972.log

# COMMAND ----------

# !cat /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/checkpoints/streams_clk_bronze_app_nhn/0/sources/0/rocksdb/logs/012935-5f2f8f49-b24b-456f-a4d3-1997cce423e7.log

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Overwrite with Partitioning

# COMMAND ----------

# df = spark.read.table("auto_report.ttt_fb_series_ad_stats")

# (df.write.format("delta")
#       .mode("overwrite")
#       .option("overwriteSchema", "true")
#       .partitionBy("dateStart")
#       .saveAsTable("auto_report.ttt_fb_series_ad_stats"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Move Files

# COMMAND ----------

# dbutils.fs.mv("/mnt/ptbwa-basic/mv-test/airbridge_properties.csv", "/FileStore/configs/airbridge_properties.csv")

# COMMAND ----------

# s3://ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=27/hour=18/

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Propfit Data Reload
# MAGIC - Click `2022-12-28`

# COMMAND ----------

# query = "delete from ice.streams_clk_bronze_app_nhn where actiontime_local >= '2022-12-28T10:53:35.5214852+00:00' and actiontime_local < '2022-12-28T11:00:00.5214852+00:00'"

# COMMAND ----------

# spark.sql(query)

# COMMAND ----------

# (!) schema
# clk_df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=27/hour=20/minute=20/streams_clk_app_nhn+0+0000011712.json.gz")

# COMMAND ----------

# for hour in range(10):
#     df = spark.read.schema(clk_df.schema).json(f"dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/year=2022/month=12/day=28/hour=0{hour}")
#     df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("ice.streams_clk_bronze_app_nhn")

# COMMAND ----------

