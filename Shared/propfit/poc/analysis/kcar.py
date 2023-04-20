# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Feature Importance on SearchUser Prediction

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### EDA

# COMMAND ----------

df = spark.sql("select * from hive_metastore.auto_report.ga_kcar_stats")

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Querying
# MAGIC - Selection
# MAGIC   - Feature
# MAGIC   - Label

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT INT(month(date)) AS month,
# MAGIC        session,
# MAGIC        user,
# MAGIC        newuser,
# MAGIC        counselpay,
# MAGIC        beforepay,
# MAGIC        simptapply,
# MAGIC        clickcall,
# MAGIC        signup,
# MAGIC        interstuser,
# MAGIC        prepurchaseuser,
# MAGIC        CASE
# MAGIC             WHEN reporttype = '내차팔기' THEN 0
# MAGIC             ELSE 1
# MAGIC        end AS reporttype,
# MAGIC        searchuser
# MAGIC FROM   hive_metastore.auto_report.ga_kcar_stats
# MAGIC WHERE  1=1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Split Dataset

# COMMAND ----------

train, test = _sqldf.randomSplit([0.8, 0.2], seed=43)

# COMMAND ----------

print("There are %d training examples and %d test examples." % (train.count(), test.count()))

# COMMAND ----------

display(train.select("month", "searchUser"))

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, VectorIndexer

featuresCols = _sqldf.columns
featuresCols.remove('reportType')
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=6)

# COMMAND ----------

featuresCols

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import LogisticRegression
 
gbt = GBTRegressor(labelCol="searchUser")

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
 
paramGrid = ParamGridBuilder()\
  .addGrid(gbt.maxDepth, [2, 5])\
  .addGrid(gbt.maxIter, [10, 100])\
  .build()
 
evaluator = RegressionEvaluator(metricName="rmse", labelCol=gbt.getLabelCol(), predictionCol=gbt.getPredictionCol())
cv = CrossValidator(estimator=gbt, evaluator=evaluator, estimatorParamMaps=paramGrid)

# COMMAND ----------

# import mlflow

# mlflow.spark.log_model(gbt, "gbt")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Training
# MAGIC - GridSearchCV

# COMMAND ----------

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# COMMAND ----------

pipelineModel = pipeline.fit(train)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Feature Importance
# MAGIC - based on Best Model

# COMMAND ----------

pipelineModel.stages[-1].bestModel.params

# COMMAND ----------

pipelineModel.stages[-1].bestModel.numFeatures

# COMMAND ----------

pipelineModel.stages[-1].bestModel.featureImportances

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Test

# COMMAND ----------

predictions = pipelineModel.transform(test)

# COMMAND ----------

display(predictions.select("searchUser", "prediction", *featuresCols))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Evaluation

# COMMAND ----------

rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)