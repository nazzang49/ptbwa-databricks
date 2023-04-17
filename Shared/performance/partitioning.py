# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Partitioning
# MAGIC - Processed by Arguments
# MAGIC - Applied to
# MAGIC   - AutoReport Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
p_log = log4jLogger.LogManager.getLogger("TABLE-PARTITIONING")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

import pprint

from enum import Enum
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

class PtbwaDataTypes(Enum):
    """
    A enum class for inserting arguments as types
    """
    DATABASE_ICE = "ice"
    DATABASE_CREAM = "cream"
    DATABASE_DEFAULT = "default"
    DATABASE_AUTOREPORT = "auto_report"

    TABLE_NAME_TYPE_FULL = "full"
    TABLE_NAME_TYPE_REGEX = "regex"
    TABLE_NAME_TYPE_CONTAINS = "contains"    

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
            p_log.info(f"[CHECK-{domain}-RESULT]{result}")
        except Exception as e:
            raise e

# COMMAND ----------

class PtbwaDataValidatior:
    """
    A processor class for validating PTBWA Delta Tables
    """
    
    _valid_databases = [
        "ice",
        "cream"
        "auto_report",
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
        
        if kwargs.get("start_date"):
            self.start_date = kwargs["start_date"]
        
        if kwargs.get("end_date"):
            self.end_date = kwargs["end_date"]
            
        if kwargs.get("row_threshold"):
            self.row_threshold = kwargs["row_threshold"]
        
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
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e
            
    def search_tables(self):
        """
        A method for searching specific tables in {self.tables}
        """
        try:
            if self.table_name_type == "full":
#                 self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name == table["tableName"] and "tt_" not in table["tableName"]]
                self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name == table["tableName"]]
            elif self.table_name_type == "contains":
                self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name in table["tableName"]]
#                 self.searched_tables = [table["tableName"] for table_name in self.table_names for table in self.tables if table_name in table["tableName"] and "tt_" not in table["tableName"]]
            elif self.table_name_type == "regex":
                pass
            else:
                raise ValueError("[INVALID-TABLE-NAME-TYPE]NOT-FOUND-TABLE-NAME-TYPE")
        except Exception as e:
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]")
            raise e

# COMMAND ----------

class PartitioningProcessor():
    
    def __init__(self, ptbwa_data_validator:PtbwaDataValidatior):
        self.ptbwa_data_validator = ptbwa_data_validator
        self.date_cols = dict()
        self.success_tables = list()
    
    def process(self):
        """
        A method for writing tables with partitioning
        """
        try:
            for table in self.ptbwa_data_validator.searched_tables:
                self.read_tables(table)
                if "row_threshold" in self.ptbwa_data_validator.__dict__ and not self.check_row_threshold(table):
                    PtbwaUtils.check_result(f"TOO-SMALL-ROW-COUNT", f"SKIP-PARTITIONING-PROCESS|TABLE::{table}")
                    continue
                self.detect_date_column(table)
                self.write_tables_with_partitioning(table)
                self.success_tables.append(table)
                
            PtbwaUtils.check_result("PARTITIONING-SUCCESS-TABLES", f"{self.success_tables}")
        except Exception as e:
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-LIST::{self.ptbwa_data_validator.searched_tables}")
            raise e
    
    def read_tables(self, table):
        """
        A method for reading tables
        """
        try:
            query = f"SELECT * FROM {self.ptbwa_data_validator.database}.{table}"
            setattr(self, f"{table}_df", spark.sql(query))
        except Exception as e:
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
            
    def check_row_threshold(self, table):
        """
        A method for checking row threshold e.g. df.count() > row_threshold
        """
        try:
            df = getattr(self, f"{table}_df")
            row_count = df.count()
            PtbwaUtils.check_result(f"ROW-COUNT|TABLE::{table}", row_count)
            return True if row_count >= self.ptbwa_data_validator.row_threshold else False
        except Exception as e:
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e        
    
    def detect_date_column(self, table):
        """
        A method for detecting date column
        """
        try:
            df = getattr(self, f"{table}_df")
            
            # (!) retrieve candidates of date-cols e.g. "start" in kakaopay-related tables
#             date_cols = [col for col in df.columns if "date" in col or "Date" in col or "start" in col]
            date_cols = [col for col in df.columns if col.startswith(("date", "Date", "start")) or col.endswith(("date", "Date", "start"))]
            
            # (!) check types e.g. string or date or timestamp
            for col in date_cols:
                if isinstance(df.schema[col].dataType, (DateType, StringType, TimestampType)):
                    self.date_cols[table] = col
                    PtbwaUtils.check_result(f"IS-DATE-COL|TABLE::{table}", col)
                    break
                else:
                    PtbwaUtils.check_result(f"NOT-DATE-COL|TABLE::{table}", col)
        except Exception as e:
            p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
            raise e
    
    def write_tables_with_partitioning(self, table):
        """
        A method for writing tables with partitioning
        """
        try:
            df = getattr(self, f"{table}_df")
            if self.date_cols.get(table):
                (df
                 .write
                 .format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .partitionBy(self.date_cols[table])
                 .saveAsTable(f"{self.ptbwa_data_validator.database}.{table}"))
            else:
                # (!) if table does not have date-cols
                PtbwaUtils.check_result(f"DATE-COLS-NOT-EXIST", table)
        except Exception as e:
            if "You may not write data into a view." in e.args[0]:
                PtbwaUtils.check_result(f"TABLE-TYPE|TABLE::{table}", "VIEW")
            else:
                p_log.error(f"[FAIL-{sys._getframe().f_code.co_name.upper()}]TABLE-NAME::{table}")
                raise e

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Processing
# MAGIC - Arguments Setup
# MAGIC - Main Process
# MAGIC   - `row_threshold` filtering tables based on row count above threshold

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (1) Row Threshold

# COMMAND ----------

# ptbwa_data_validator = PtbwaDataValidatior(PtbwaDataTypes.DATABASE_AUTOREPORT.value, PtbwaDataTypes.TABLE_NAME_TYPE_CONTAINS.value, ["gad_", "fb_", "twt_", "kmt_", "airb"], row_threshold=10000)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (2) Default
# MAGIC - table_names e.g. `[kakaopay_report_d]`

# COMMAND ----------

# ptbwa_data_validator = PtbwaDataValidatior(PtbwaDataTypes.DATABASE_AUTOREPORT.value, PtbwaDataTypes.TABLE_NAME_TYPE_CONTAINS.value, ["kakaopay_report_d"])

# COMMAND ----------

ptbwa_data_validator.set_up()

# COMMAND ----------

ptbwa_data_validator.searched_tables

# COMMAND ----------

partitioning_processor = PartitioningProcessor(ptbwa_data_validator)

# COMMAND ----------

partitioning_processor.process()

# COMMAND ----------

# df = spark.sql("select * from auto_report.tt_gad_kakaopay_video_stats_after where segmentDate = '2022-09-19'")
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### How to Detect Date Columns in Whole Tables
# MAGIC - (1) Check to_date() is Possible
# MAGIC   - Exception Occur on `auto_report.tv_gad_kakaopay_advideo_stats_d.DateInt`
# MAGIC - (2) Simple if-else Conditions on Schema
# MAGIC - (3) Regex
# MAGIC - (4) startswith + endswith

# COMMAND ----------

# funcs = to_date("DateInt").alias("DateInt")

# COMMAND ----------

# df = spark.sql("select * from auto_report.tv_gad_kakaopay_advideo_stats_d limit 1")

# COMMAND ----------

# df = df.withColumn("DateInt", funcs)

# COMMAND ----------

# if isinstance(df.schema["DateInt"].dataType, (IntegerType, StringType)):
#     print("들어온다")

# COMMAND ----------

# (!) check prefix
# import re
# p = re.compile('[d|D]ate[a-zA-Z]+')
# m = p.match("DateStart")
# print(m)

# COMMAND ----------

# (!) check suffix e.g. "$" means end
# import re
# p = re.compile('[a-zA-Z]+[d|D]ate$')
# m = p.match("metricsupdate")
# print(m)
