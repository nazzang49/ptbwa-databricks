# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Test on Synthetic Data using XGBoost Classification
# MAGIC - Based on Migun
# MAGIC - Init `2023-04-12`
# MAGIC - Filters
# MAGIC   - (수동입찰 기간) `2023-03-03` - `2023-03-13`
# MAGIC - Assignee @jinyoung.park
# MAGIC - Synthetic Types
# MAGIC   - Databricks DBGen
# MAGIC   - SDV

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from enum import Enum
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import *
from sparkdl.xgboost import *
from pyspark.ml.regression import *
from pyspark.ml.feature import *
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from datetime import datetime

import re
import os
import dbldatagen as dg

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Enum

# COMMAND ----------

class PropfitCommonType(Enum):
    """
    A class for using propfit types e.g. ml
    """
    
    PROPFIT = "propfit"
    DMP = "dmp"
    GA4 = "ga4"
    JOIN = "join"
    JOIN_V2 = "join_v2"
    DATABRICKS = "databricks"
    SDV = "sdv"

    # (!) check cluster configuration (8core * worker nodes)
    NUM_OF_REPARTITION = 40

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Generate Android ID
# MAGIC - uuid

# COMMAND ----------

# import uuid

# device_id = str(uuid.uuid4())
# print(device_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Query

# COMMAND ----------

class MyQuery:
    """
    A class for creating queries based on data types
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return ('Field('
                # f'table={self.table!r},'
                ')')

    @classmethod
    def get_query(cls, query_type: str, advertiser: str = None):
        try:
            query_type = getattr(PropfitCommonType, query_type.upper()).value
            base_path = os.path.join("/dbfs/FileStore/sql", advertiser if advertiser else "")
            file_path = os.path.join(base_path, f"{query_type}.sql")

            with open(file_path, 'r') as f:
                query = f.readlines()
            return f"""{''.join(query)}"""
        except Exception:
            raise ValueError(f"[NOT_FOUND_QUERY]DATA_TYPE::{query_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Validator

# COMMAND ----------

class MyValidator:
    """
    A class for creating validator to check arguments
    """

    _REQUIRED_DATAFRAMES = [
        "df_propfit",
        "df_dmp",
        "df_ga4",
    ]

    _CANDIDATES_CASES = {
        "camel": [
            "snake"
        ],
        "snake": [
            "camel"
        ],
    }

    _CANDIDATES_DATASET_TYPES = [
        "databricks",
        "sdv"
    ]

    _CANDIDATES_MODEL_TYPES = [
        "randomforestclassifier",
        "randomforestregressor",
        "xgboostclassifier",
        "xgboostregressor"
    ]

    def __init__(self) -> None:
        pass

    def __repr__(self) -> str:
        return super().__repr__()

    @staticmethod
    def is_valid_dataframes(**kwargs):
        for dataframe in MyValidator._REQUIRED_DATAFRAMES:
            if dataframe not in kwargs:
                raise ValueError(f"[NOT_FOUND_{dataframe.upper()}]REQUIRED")

    @staticmethod
    def is_valid_cases(before: str, after: str):
        if before not in MyValidator._CANDIDATES_CASES:
            raise ValueError(f"[NOT_FOUND_{before.upper()}]BEFORE_CASE")

        if after not in MyValidator._CANDIDATES_CASES[before]:
            raise ValueError(f"[NOT_FOUND_{after.upper()}]AFTER_CASE")

    @staticmethod
    def is_valid_dataset_type(dataset_type: str):
        if dataset_type not in MyValidator._CANDIDATES_DATASET_TYPES:
            raise ValueError(f"[NOT_FOUND_{dataset_type.upper()}]INVALID")

    @staticmethod
    def is_valid_dataframe(df: dataframe.DataFrame):
        if not isinstance(df, dataframe.DataFrame):
            raise ValueError(f"[INVALID_TYPE]{type(df)}")

    @staticmethod
    def is_valid_model_type(model_type: str):
        if model_type not in MyValidator._CANDIDATES_MODEL_TYPES:
            raise ValueError(f"[INVALID_MODEL_TYPE]{model_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Utils

# COMMAND ----------

class MyUtils:
    """
    A class for creating util functions
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

    @staticmethod
    def _convert_camel_to_snake(col: str):
        try:
            snaked_col = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col)
            snaked_col = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snaked_col)
            return snaked_col.lower()
        except Exception as e:
            print(f"[FAIL_CONVERT_CAMEL_TO_SNAKE]COLUMN_NAME::{col}")
            raise e

    @staticmethod
    def convert_case(df: dataframe.DataFrame, before: str, after: str):
        try:
            MyValidator.is_valid_cases(before, after)

            for col in df.columns:
                case_converted_col = getattr(MyUtils, f"_convert_{before}_to_{after}")(col)
                df = df.withColumnRenamed(col, case_converted_col)
            return df
        except Exception as e:
            print("[FAIL_CONVERT_CASE]")
            raise e

    @staticmethod
    def summarize_dataset(df: dataframe.DataFrame):
        try:
            MyValidator.is_valid_dataframe(df)

            return dg.DataAnalyzer(sparkSession=spark, df=df).summarizeToDF()
        except Exception as e:
            print("[FAIL_SUMMARIZE_DATASET]")
            raise e

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Load Dataset based on Baseline
# MAGIC - DataTypes
# MAGIC   - Propfit
# MAGIC   - TG360
# MAGIC   - GA4

# COMMAND ----------

class MyDataset:
    """
    A class for creating synthetic dataset based on joining data types
    """
    def __init__(self, **kwargs) -> None:
        MyValidator.is_valid_dataframes(**kwargs)

        self.__dict__.update(kwargs)
        print(f"[MYDATASET_ATTRIBUTIONS]{self.__dict__}")

    def __repr__(self) -> str:
        return super().__repr__()

    def create_views(self):
        """
        A method for creating views of dataframes
        """
        for k, v in self.__dict__.items():
            try:
                if isinstance(v, dataframe.DataFrame):
                    v.createOrReplaceTempView(f"{k}_view")  # e.g. df_dmp_view
            except Exception as e:
                print(f"[FAIL_CREATE_VIEW]TABLE::{k}")

    def create_dataset(self, version:str = None):
        """
        A method for creating train dataset by joining dataframes
        """
        try:
            query_type = f"{PropfitCommonType.JOIN.value}_{version}" if version else f"{PropfitCommonType.JOIN.value}"
            query = MyQuery.get_query(query_type, "migun")

            self.df = spark.sql(query)
        except Exception as e:
            print(f"[FAIL-JOIN-PROCESS]")
            raise e

    def create_dataset_with_dataframe(self):
        """
        A method for creating train dataset by joining dataframes
        """
        try:
            # self.df_dmp
            # self.df_ga4
            # self.df_propfit

            # (!) join :: propfit - dmp (tg)
            self.df_join = self.df_propfit.alias('a').join(self.df_dmp, self.df_propfit.device_id == self.df_dmp.uuid, 'left')
            self.df_join = self.df_join.select(
                col('a.*'),
                col('gender_code').alias('gender'), 
                col('age_range').alias('age'), 
                col('carrier')
            ).withColumn("gender", col('gender').cast(StringType()))

            # (!) join :: propfit - dmp (tg) - ga4
            self.df_join = self.df_join.alias('a')
            self.df_ga4 = self.df_ga4.alias('b')
            self.df_join = self.df_join.join(self.df_ga4, (self.df_join.tid == self.df_ga4.tid) & (self.df_join.device_id == self.df_ga4.device_id), 'left')

            self.df = (
                self.df_join.select(
                    col('a.date'), 
                    dayofweek('a.date').alias('dow'),
                    col('a.device_id'), 
                    col('a.tid'), 
                    col('user_pseudo_id'),
                    col('bid'), 
                    col('imp'), 
                    col('clk'),
                #                          col('ad_adv'),
                    col('gender'), 
                    col('age'), 
                    col('carrier'),
                    col('size'), 
                    col('bidfloor'), 
                    col('app_category'),
                    col('device_type'), 
                    col('os'), 
                    col('region'),
                    col('revenue'), 
                    col('itct'), 
                    col('new_user'), 
                    col('session_duration'),
                    col('pageview_event'), 
                    col('s0_event'), 
                    col('s25_event'), 
                    col('s50_event'), 
                    col('s75_event'), 
                    col('s100_event'),
                    col('cv'), 
                    col('clk_request_miso_event'), 
                    col('clk_csr_event'), 
                    col('clk_request_lower_event')
                )
                    .withColumn("dow", col('dow').cast(StringType()))
                    .withColumn("bidfloor", col('bidfloor').cast(DoubleType()))
                    .withColumn("bid", col('bid').cast(StringType()))
                    .withColumn("imp", col('imp').cast(StringType()))
                    .withColumn("clk", col('clk').cast(StringType()))
                    .withColumn("new_user", col('new_user').cast(StringType()))
                    .withColumn("device_type", col('device_type').cast(StringType()))
            )

        except Exception as e:
            print(f"[FAIL-JOIN-PROCESS]")
            raise e

    def _create_databricks_synthetics(self):
        """
        A method for creating synthetics by dbldatagen
        """
        try:
            analyzer = dg.DataAnalyzer(sparkSession=spark, df=self.df)
            self.synthetics = analyzer.scriptDataGeneratorFromData()
        except Exception as e:
            raise e

    def _create_sdv_synthetics(self):
        try:
            pass
        except Exception as e:
            raise e

    def create_synthetics(self, dataset_type: str):
        """
        A method for creating synthetics by dataset_type
        dataset_type:
            databricks (dbldatagen)
            sdv
        """
        MyValidator.is_valid_dataset_type(dataset_type)

        getattr(self, f"_create_{dataset_type}_synthetics")()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Preprocessor

# COMMAND ----------

class MyPreprocessor:
    """
    A class for creating preprocessor to clean dataset
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Visualizer

# COMMAND ----------

class MyVisualizer:
    """
    A class for creating visualizer to eda
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Model

# COMMAND ----------

class MyModel:
    """
    A class for creating model based on regression and classification
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()
    
    def _get_xgboostclassifier(**kwargs):
        pass

    def _get_xgboostregressor(**kwargs):
        pass

    def _get_randomforestclassifier(**kwargs):
        pass

    def _get_randomforestregressor(**kwargs):
        pass

    @classmethod
    def get_model(cls, model_type: str = None, **kwargs):
        """
        A method for getting model based on spark mllib e.g. randomforestclassifier
        """
        MyValidator.is_valid_model_type(model_type)

        return getattr(MyModel, f"_get_{model_type}")(kwargs)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Trainer

# COMMAND ----------

class MyTrainer:
    """
    A class for creating trainer
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Evaluator

# COMMAND ----------

class MyEvaluator:
    """
    A class for creating evaluator
    """

    def __init__(self) -> None:
        super().__init__()

    def __repr__(self) -> str:
        return super().__repr__()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Dataframes by SQL Files
# MAGIC - `/dbfs/FileStore/sql/*`
# MAGIC - (Optional) Repartition

# COMMAND ----------

query = MyQuery.get_query(PropfitCommonType.DMP.value)
df_dmp = spark.sql(query)
df_dmp = MyUtils.convert_case(df_dmp, "camel", "snake")
# df_dmp = df_dmp.repartition(PropfitCommonType.NUM_OF_REPARTITION.value)

print(query)
print(df_dmp.printSchema())

query = MyQuery.get_query(PropfitCommonType.GA4.value)
df_ga4 = spark.sql(query)
df_ga4 = MyUtils.convert_case(df_ga4, "camel", "snake")
# df_ga4 = df_ga4.repartition(PropfitCommonType.NUM_OF_REPARTITION.value)

print(query)
print(df_ga4.printSchema())

query = MyQuery.get_query(PropfitCommonType.PROPFIT.value)
df_propfit = spark.sql(query)
df_propfit = MyUtils.convert_case(df_propfit, "camel", "snake")
# df_propfit = df_propfit.repartition(PropfitCommonType.NUM_OF_REPARTITION.value)

print(query)
print(df_propfit.printSchema())

dataframes = {
    "df_dmp": df_dmp,
    "df_ga4": df_ga4,
    "df_propfit": df_propfit,
}

# COMMAND ----------

# df_propfit.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### (Optional) Join without SQL Files
# MAGIC - for TroubleShooting

# COMMAND ----------

df_join.printSchema()

# COMMAND ----------

# df_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Dataset
# MAGIC - Dataset Spec
# MAGIC   - https://ptbwa.atlassian.net/wiki/spaces/PROP/pages/121733139/230317
# MAGIC - Real Dataset
# MAGIC   - Propfit
# MAGIC   - TG360
# MAGIC   - GA4
# MAGIC - Synthetic Dataset
# MAGIC   - by `dbldatagen`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Real Dataset

# COMMAND ----------

dataset = MyDataset(**dataframes)

# (!) -- join result is different => check! --
dataset.create_views()
dataset.create_dataset_with_dataframe()

# COMMAND ----------

# (!) (optional) if imp exist from propfit | dataset
# df_propfit.filter("imp = '0'").display()
dataset.df.filter("imp = '0'").display()

# COMMAND ----------

# dataset.df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Extract Values
# MAGIC - App Categories
# MAGIC - Device Types
# MAGIC - OS Types
# MAGIC - Regions

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     app_category,
# MAGIC     count(*) as cnt,
# MAGIC     (COUNT(*) / SUM(COUNT(*)) OVER ()) * 100 as ratio
# MAGIC FROM      df_propfit_view
# MAGIC WHERE     app_category != ''
# MAGIC AND       app_category is not null
# MAGIC GROUP BY  app_category

# COMMAND ----------

app_categories = _sqldf.collect()
app_categories = {
    app_category["app_category"]: app_category["ratio"] for app_category in app_categories
}

# COMMAND ----------

app_categories

# COMMAND ----------

# (!) by custom

# device_types = {
#     "0": "",
#     "1": "Phone", 
#     "2": "Mobile/Tablet", 
#     "3": "Tablet", 
# }

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- (!) by sql
# MAGIC -- extract {0=Unknown} {1=Mobile/Tablet} {4=Phone} {5=Tablet}
# MAGIC 
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC       WHEN device_type = '0' THEN '1'
# MAGIC       WHEN device_type = '1' THEN '2'
# MAGIC       WHEN device_type = '4' THEN '3'
# MAGIC       WHEN device_type = '5' THEN '4'
# MAGIC       ELSE device_type END AS device_type,
# MAGIC     count(*) as cnt,
# MAGIC     (COUNT(*) / SUM(COUNT(*)) OVER ()) * 100 as ratio
# MAGIC FROM      df_propfit_view
# MAGIC WHERE     device_type IN ('0', '1', '4', '5')
# MAGIC GROUP BY  device_type

# COMMAND ----------

device_types = _sqldf.collect()
device_types = {
    device_type["device_type"]: device_type["ratio"] for device_type in device_types
}

# COMMAND ----------

device_types

# COMMAND ----------

# (!) by custom

# os = {
#     "1": "android",
#     "2": "ios",
#     "3": "unknown",
# }

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- (!) by sql
# MAGIC -- extract {1=android} {2=ios} {3=unknown}
# MAGIC 
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC       WHEN os = '1' THEN '1'
# MAGIC       WHEN os = '2' THEN '2'
# MAGIC       WHEN os = '4' THEN '3'
# MAGIC       ELSE os END AS os,
# MAGIC     count(*) as cnt,
# MAGIC     (COUNT(*) / SUM(COUNT(*)) OVER ()) * 100 as ratio
# MAGIC FROM      df_propfit_view
# MAGIC WHERE     os IN ('1', '2', '4')
# MAGIC GROUP BY  os

# COMMAND ----------

os_types = _sqldf.collect()
os_types = {
    os["os"]: os["ratio"] for os in os_types
}

# COMMAND ----------

os_types

# COMMAND ----------

# (!) by file

# df_region = (
#     spark
#         .read
#         .option("header", "true")
#         .csv("dbfs:/FileStore/propfit/region_ratio.csv")
#         .withColumnRenamed("((count(1) / 42679893) * 100)", "ratio")
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- (!) by sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     region,
# MAGIC     count(*) as cnt,
# MAGIC     (COUNT(*) / SUM(COUNT(*)) OVER ()) * 100 as ratio
# MAGIC FROM      df_propfit_view
# MAGIC WHERE     region != ''
# MAGIC AND       region is not null
# MAGIC GROUP BY  region

# COMMAND ----------

regions = _sqldf.collect()
regions = {
    region["region"]: float(region["ratio"]) for region in regions if region["region"]
}

# COMMAND ----------

regions

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Synthetic Dataset
# MAGIC 1. (Optional - Just for Init) Set Spec
# MAGIC 2. Generation

# COMMAND ----------

# summarized_df = MyUtils.summarize_dataset(dataset.df)
# display(summarized_df)
# dataset.create_synthetics(PropfitCommonType.DATABRICKS.value)

# COMMAND ----------

# dataset.synthetics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Data Spec
# MAGIC - Init
# MAGIC   - @hyein.wang
# MAGIC - V1 
# MAGIC   - `2023-04-14` @jinyoung.park
# MAGIC   - Caution on Ratio Related Columns
# MAGIC     - `device_type`
# MAGIC     - `os`
# MAGIC     - `app_category`
# MAGIC     - `region`
# MAGIC - Basics
# MAGIC   - Required Revision on `WithColumn Details`
# MAGIC   - https://github.com/databrickslabs/dbldatagen/blob/615c56b4a3de42be5948b753944e29aa98834662/dbldatagen/data_generator.py#L738
# MAGIC   - https://databrickslabs.github.io/dbldatagen/public_docs/DATARANGES.html

# COMMAND ----------

generation_spec = (
    dg.DataGenerator(sparkSession=spark, 
                     name='synthetic_data', 
                     rows=100,
                     random=True,
                     )
    # (!) date range on 2023-03-XX
    .withColumn('date', 'timestamp', data_range=dg.DateRange("2023-03-03 00:00:00", "2023-03-13 11:59:00", "hours=1"))
    
    # (!) 1=sun, 2=mon, 3=tue, 4=wed, 5=thu, 6=fri, 7=sat
    # Q) weekends vs weekdays?
    .withColumn('dow', 'string', values=['1', '2', '3', '4', '5', '6', '7'], weights=[1, 5, 5, 5, 5, 5, 1])

    # (!) sql expr
    .withColumn('device_id', 'string', expr="uuid()")

    # (!) drop
    # Q) required?
    # Q) pattern from bidder | ga4
    .withColumn('tid', 'string', template=r'\\w')
    # .withColumn('user_pseudo_id', 'string', template=r'\\w', percentNulls=0.52)

    # (!) existance
    .withColumn('bid', 'string', values=['1'], percentNulls=0)
    .withColumn('imp', 'string', values=['0', '1'], weights=[1, 10])
    .withColumn('clk', 'string', values=['0', '1'], weights=[1, 2])

    # (!) demographic from dmp
    .withColumn('gender', 'string', values=['0', '1'], weights=[1, 1], percentNulls=0)
    .withColumn('age', 'string', values=['10s', '20s', '30s', '40s', '50s', '60s'], percentNulls=0)
    .withColumn('carrier', 'string', values=['70540', '70541', '70542'], percentNulls=0)

    # (!) banner size from propfit
    # 1. 320 * 50
    # 2. 320 * 480
    .withColumn('size', 'string', values=['1', '2'], weights=[5, 1])

    # (!) bidfloor '2023-03-03' - '2023-03-13' from propfit
    .withColumn('bidfloor', 'double', minValue=0.08, maxValue=0.5, step=0.001)

    # (!) distinct app_category from propfit
    .withColumn(
        'app_category', 
        'string', 
        values=list(app_categories.keys()), 
        weights=list(app_categories.values()), 
        percentNulls=0.06
    )

    # (!) 0=?, 1=Phone, 2=Mobile/Tablet, 3=Tablet
    # Q) 0? mobile skeweness?
    .withColumn(
        'device_type', 
        'string', 
        values=list(device_types.keys()), 
        weights=list(device_types.values()), 
        percentNulls=0
    )

    # (!) 1=android, 2=ios, 3=unknown
    # Q) android skewness?
    .withColumn(
        'os', 
        'string', 
        values=list(os_types.keys()), 
        weights=list(os_types.values()), 
        percentNulls=0
    )

    # (!) /dbfs/FileStore/propfit/region_ratio.csv
    .withColumn(
        'region', 
        'string', 
        values=list(regions.keys()), 
        weights=list(regions.values()), 
        percentNulls=0.5
    )

    # (!) revenue '2023-03-03' - '2023-03-13' from ga?
    # Q) step?
    .withColumn('revenue', 'double', minValue=7.558578987150416E-5, maxValue=4.4978695E-4, step=0.00005)

    # (!) itct > 0
    # Q) itct < 0 fraud? e.g. -149
    .withColumn('itct', 'bigint', minValue=0, maxValue=56761)
    .withColumn('new_user', 'string', values=["0", "1"], weights=[1, 2], percentNulls=0.52)
    
    # (!) from ga
    .withColumn('session_duration', 'bigint', minValue=0, maxValue=20724, percentNulls=0.52)
    .withColumn('pageview_event', 'bigint', minValue=0, maxValue=57, percentNulls=0.52)
    .withColumn('s0_event', 'bigint', minValue=0, maxValue=0, percentNulls=0.52)
    .withColumn('s25_event', 'bigint', minValue=0, maxValue=4, percentNulls=0.52)
    .withColumn('s50_event', 'bigint', minValue=0, maxValue=3, percentNulls=0.52)
    .withColumn('s75_event', 'bigint', minValue=0, maxValue=2, percentNulls=0.52)
    .withColumn('s100_event', 'bigint', minValue=0, maxValue=0, percentNulls=0.52)
    .withColumn('cv', 'decimal(2,1)', minValue=0.0, maxValue=1.0)
    .withColumn('clk_request_miso_event', 'bigint', minValue=0, maxValue=18, percentNulls=0.52)
    .withColumn('clk_csr_event', 'bigint', minValue=0, maxValue=3, percentNulls=0.52)
    .withColumn('clk_request_lower_event', 'bigint', minValue=0, maxValue=1, percentNulls=0.52)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Generation

# COMMAND ----------

output = generation_spec.build()

# COMMAND ----------

# drop_cols = [
#     "device_id",
#     "tid",
#     "user_pseudo_id"
# ]

# df = output.drop(*drop_cols)

# COMMAND ----------

output.display()

# COMMAND ----------

output.createOrReplaceTempView("synthetic_view")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Postprocess
# MAGIC - Event Time `bid > imp > clk`
# MAGIC - Check & Validate Values on Columns
# MAGIC   - `device_type`
# MAGIC   - `os`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM  synthetic_view
# MAGIC WHERE (imp = '0' and clk = '0')
# MAGIC OR    (imp = '1' and clk = '0')
# MAGIC OR    (imp = '1' and clk = '1')

# COMMAND ----------

df_synthetic = _sqldf

# COMMAND ----------

# %sql

# SELECT *
# FROM synthetic_view
# WHERE device_type NOT IN ('1', '2', '3', '4')
# OR os NOT IN ('1', '2', '3')

# COMMAND ----------

# if _sqldf.count() > 0:
#     print("[INVALID_VALUE_DETECTED]DEVICE_TYPE_OR_OS")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Combine Original and Synthetic
# MAGIC - Union
# MAGIC - Select & Drop

# COMMAND ----------

dataset.df = dataset.df.drop("user_pseudo_id")

# COMMAND ----------

# (!) combine original + synthetic
# df_combine = dataset.df.union(df_synthetic)

# (!) only original
df_combine = dataset.df

# COMMAND ----------

# df_combine.printSchema()

# COMMAND ----------

df_combine_cv = df_combine.select("cv", "tid")
df_combine = df_combine.drop("date", "bid", "cv")

# COMMAND ----------

# (!) features
df_combine.cache()

# (!) labels
df_combine_cv.cache()

# COMMAND ----------

print("The dataset has %d rows." % df_combine.count())

# COMMAND ----------

df_combine.createOrReplaceTempView("df_combine_view")

# COMMAND ----------

from collections import defaultdict

data_types = defaultdict(list)
for field in df_combine.schema.fields:
    data_type = str(field.dataType).replace("()", "")
    data_type = MyUtils._convert_camel_to_snake(data_type).split("_")[0]

    # (!) {cv} is decimal
    if data_type != 'decimal':
        data_types[data_type].append(field.name)

# COMMAND ----------

data_types

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Categorical
# MAGIC - String

# COMMAND ----------

# (!) baseline
# [
#     'dow',
#     'imp',
#     'clk',
#     'gender',
#     'age',
#     'carrier',
#     'size',
#     'appCategory',
#     'deviceType',
#     'os',
#     'region',
#     'newUser',
#     'cv'
#  ]

# COMMAND ----------

string_cols = [string_col for string_col in data_types["string"] if string_col not in ["device_id", "tid"]]
string_cols

# COMMAND ----------

counted_string_cols = [f"count(distinct {string_col})" for string_col in string_cols]

# COMMAND ----------

counted_string_cols = ', '.join(counted_string_cols)
counted_string_cols

# COMMAND ----------

############################################################## 
# (!) (optional) check too many distinct values in string cols
##############################################################

query = f"""
   select {counted_string_cols}
   from df_combine_view
"""

# COMMAND ----------

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Fill Missing Values

# COMMAND ----------

df_combine = df_combine.fillna({
    col: "missing" for col in string_cols
})

# COMMAND ----------

# df_combine.filter("imp = 2").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### OHE

# COMMAND ----------

string_cols_indexer = [
    StringIndexer(inputCol=string_col, outputCol=f"{string_col}_string_encoded") for string_col in string_cols
]

string_cols_one_hot = [
    OneHotEncoder(inputCol=f"{string_col}_string_encoded", outputCol=f"{string_col}_one_hot") for string_col in string_cols
]

# COMMAND ----------

string_cols_indexer

# COMMAND ----------

string_cols_one_hot

# COMMAND ----------

ppl = Pipeline(stages=(string_cols_indexer + string_cols_one_hot))
df_combine = ppl.fit(df_combine).transform(df_combine)

# COMMAND ----------

df_combine.display()

# COMMAND ----------

# %sql

# SELECT max(bidfloor)
# FROM df_combine_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Numeric
# MAGIC - Double
# MAGIC - Long
# MAGIC   - Cast to Double

# COMMAND ----------

double_cols = data_types["double"]
double_cols

# COMMAND ----------

imputed_double_cols = [f"{double_col}_imputed" for double_col in double_cols]
imputer = Imputer(inputCols=double_cols, outputCols=imputed_double_cols)
df_combine = imputer.fit(df_combine).transform(df_combine)

# COMMAND ----------

long_cols = data_types["long"]
long_cols

# COMMAND ----------

# (!) cast long to double
for long_col in long_cols:
    df_combine = df_combine.withColumn(f"{long_col}_cast_to_double", df_combine[long_col].cast("double"))

# COMMAND ----------

double_casted_cols = [col for col in  df_combine.columns if col.endswith("_cast_to_double")]
imputed_double_casted_cols = [f"{double_casted_col}_imputed" for double_casted_col in double_casted_cols]

imputer = Imputer(inputCols=double_casted_cols, outputCols=imputed_double_casted_cols)
df_combine = imputer.fit(df_combine).transform(df_combine)

# COMMAND ----------

# df_combine.select(
#     "clk_request_lower_event", 
#     "clk_request_lower_event_cast_to_double", 
#     "clk_request_lower_event_cast_to_double_imputed" # (!) missing values transformed into specific values
# ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Final Dataset
# MAGIC - Vectorize
# MAGIC - Selection

# COMMAND ----------

# (!) select newly added columns based on categorical / mumeric
features = imputed_double_cols + imputed_double_casted_cols + [f"{string_col}_one_hot" for string_col in string_cols]

# (!) remove conversion-related features e.g. clk_csr





vector_assembler = VectorAssembler(inputCols=features, outputCol="features")

# COMMAND ----------

vector_assembler

# COMMAND ----------

vectorized_dataset = vector_assembler.transform(df_combine)

# COMMAND ----------

vectorized_dataset.select("tid").distinct().count()

# COMMAND ----------

df_combine_cv.select("tid").count()

# COMMAND ----------

vectorized_dataset = vectorized_dataset.join(df_combine_cv.alias('temp'), on=["tid"], how='left')
vectorized_dataset = vectorized_dataset.drop("temp.tid")
vectorized_dataset = vectorized_dataset.na.fill(value=2.0, subset=["cv"])

# COMMAND ----------





############################ Feature 생성 시, 전환 컬럼 제외 해야 함! ############################






# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create Feature Store

# COMMAND ----------

from databricks import feature_store
from databricks.feature_store import feature_table, FeatureLookup

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# (!) check if big dataframe is saved rightly

fs.create_table(
    name="propfit_cv_v1",
    # primary_keys=["wine_id"],
    df=vectorized_dataset,
    schema=vectorized_dataset.schema,
    description="A vectorized_dataset for training and testing lots of models."
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Split into Train and Test
# MAGIC - 8 : 2

# COMMAND ----------

train, test = vectorized_dataset.randomSplit([0.8, 0.2], seed=43)
print("There are %d training examples and %d test examples." % (train.count(), test.count()))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Train by Pipeline
# MAGIC - (Optional) Using MyModel Class to Get Spark-MLlib Models

# COMMAND ----------

# (!) (optional) from MyModel
# rf = RandomForestClassifier(labelCol="cv", featuresCol="features", numTrees=20)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Distributed Training
# MAGIC - `num_workers` with Databricks Runtime for Machine Learning 9.0 ML or Above

# COMMAND ----------

xgb_classifier = XgboostClassifier(num_workers=4, labelCol="cv", featuresCol="features", missing=0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cross Validation
# MAGIC - HPO Range Setup (based on xgb_regressor)
# MAGIC   - `maxDepth` Max Depth of Decision Tree
# MAGIC   - `maxIter` Iterations or Total Number of Trees

# COMMAND ----------

param_grid = (
    ParamGridBuilder()
    .addGrid(xgb_classifier.max_depth, [2, 5])
    .addGrid(xgb_classifier.n_estimators, [10, 100])
    .build()
)

evaluator = MulticlassClassificationEvaluator(
    labelCol=xgb_classifier.getLabelCol(),
    predictionCol=xgb_classifier.getPredictionCol()
)

cv = CrossValidator(estimator=xgb_classifier, evaluator=evaluator, estimatorParamMaps=param_grid)

# COMMAND ----------

pipeline = Pipeline(stages=[cv])

# COMMAND ----------

pipeline.explainParams()

# COMMAND ----------

pipeline_model = pipeline.fit(train)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### Predictions & Evaluations

# COMMAND ----------

predictions = pipeline_model.transform(test)

# COMMAND ----------

# predictions.select("features").display()

# COMMAND ----------


from itertools import chain

list_featureImportances_attr = list(chain(*list(predictions.schema["features"].metadata["ml_attr"]["attrs"].values())))
attrs = sorted((attr['idx'], attr['name']) for attr in list_featureImportances_attr)

# list_featureImportances = [(name, rf_model.featureImportances[idx]) for idx, name in attrs if rf_model.featureImportances[idx]]
# feature_importance = pd.DataFrame(list_featureImportances)
# feature_importance.columns = ['names', 'importance']

# COMMAND ----------

attrs

# COMMAND ----------

display(predictions.select("cv", "prediction"))

# COMMAND ----------

acc = evaluator.evaluate(predictions)
print("ACC on our test set: %g" % acc)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Feature Importance

# COMMAND ----------

pipeline_model.stages[-1].bestModel.get_feature_importances()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### (Optional) Save & Reload Model

# COMMAND ----------

# %sh
# rm -rf /dbfs/tmp/xgboost/pipeline_001
# rm -rf /dbfs/tmp/xgboost/pipelineModel_001

# COMMAND ----------

# save pipeline
# pipeline.save('/tmp/xgboost/pipeline_001')

# save model
# pipelineModel.save('/tmp/xgboost/pipelineModel_001')

# COMMAND ----------

# load pipeline
# loaded_pipeline = Pipeline.load('/tmp/xgboost/pipeline_001')

# load model
# loaded_pipelineModel = PipelineModel.load('/tmp/xgboost/pipelineModel_001')

# COMMAND ----------

# new_data = test.limit(3)
# new_preds = loaded_pipelineModel.transform(new_data)
# display(new_preds.select("cv", "prediction"))

# COMMAND ----------

