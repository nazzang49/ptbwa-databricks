# Databricks notebook source
# MAGIC %md 
# MAGIC ### Querying the event log
# MAGIC 
# MAGIC A Delta Live Tables event log is created and maintained for every pipeline and serves as the single source of truth for all information related to the pipeline, including audit logs, data quality checks, pipeline progress, and data lineage. The event log is exposed via the `/events` API and is displayed in the Delta Live Tables UI. The event log is also stored as a delta table and can be easily accessed in a Databricks Notebook to perform more complex analyis. This notebook demonstrates simple examples of how to query and extract useful data from the event log.
# MAGIC 
# MAGIC This notebook requires Databricks Runtime 8.1 or above to access the JSON SQL operators that are used in some queries.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Custom Logging
# MAGIC - to Datadog

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
w_log = log4jLogger.LogManager.getLogger("PROPFIT-DLT")

# COMMAND ----------

# DBTITLE 1,Create the storage location text box
# Creates the input box at the top of the notebook.
dbutils.widgets.text('storage', 'dbfs:/pipelines/fe789ff7-e646-481c-9ad2-49ad7b814d47', 'Storage Location')

# COMMAND ----------

# DBTITLE 1,Event log view
# MAGIC %md
# MAGIC The examples in this notebook use a view named `event_log_raw` to simplify queries against the event log. To create the `event_log_raw` view:
# MAGIC 
# MAGIC 1. Enter the path to the event log in the **Storage Location** text box. To find the path, click **Settings** on the **Pipeline details** page for your pipeline to display the **Edit pipeline settings** dialog. The path is the value in the **Storage location** field. The path is also available in the JSON settings. Click the **JSON** button on the **Edit pipeline settings** dialog and look for the `storage` field.
# MAGIC 2. Run the following commands to create the `event_log_raw` view.

# COMMAND ----------

# DBTITLE 1,Create the event log view
# Replace the text in the Storage Location input box to the desired pipeline storage location. This can be found in the pipeline configuration under 'storage'.

# storage_location = dbutils.widgets.get('storage')
storage_location = "dbfs:/pipelines/e8b9839c-b223-4c05-bf87-d9d9fa471305"
event_log_path = storage_location + "/system/events"

# Read the event log into a temporary view so it's easier to query.
event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

# COMMAND ----------

# DBTITLE 1,Event log schema
# MAGIC %md
# MAGIC The following is the top level schema for the event log:
# MAGIC 
# MAGIC | Field       | Description |
# MAGIC | ----------- | ----------- |
# MAGIC | id          | The ID of the pipeline. |
# MAGIC | sequence    | JSON containing metadata for the sequencing of events. This can be used to understand dependencies and event order. |
# MAGIC | origin      | JSON containing metadata for the origin of the event, including the cloud, region, user ID, pipeline ID, notebook name, table names, and flow names. |
# MAGIC | timestamp   | Timestamp at which the event was recorded. |
# MAGIC | message     | A human readable message describing the event. This message is displayed in the event log component in the main DLT UI. |
# MAGIC | level       | The severity level of the message. |
# MAGIC | error       | If applicable, an error stack trace associated with the event. |
# MAGIC | details     | JSON containing structured details of the event. This is the primary field for creating analyses with event log data. |
# MAGIC | event_type  | The event type. |

# COMMAND ----------

# DBTITLE 1,View a sample of event log records
# MAGIC %sql
# MAGIC SELECT * FROM event_log_raw
# MAGIC WHERE event_type = "flow_progress"
# MAGIC AND error is not null
# MAGIC LIMIT 1000

# COMMAND ----------

# DBTITLE 1,Audit Logging
# MAGIC %md
# MAGIC A common and important use case for data pipelines is to create an audit log of actions users have performed. The events containing information about user actions have the event type `user_action`. The following is an example to query the timestamp, user action type, and the user name of the person taking the action with the following:

# COMMAND ----------

# DBTITLE 1,Example query for user events auditing
# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name FROM event_log_raw WHERE event_type = 'user_action'

# COMMAND ----------

# DBTITLE 1,Pipeline update details
# MAGIC %md
# MAGIC Each instance of a pipeline run is called an *update*. The following examples extract information for the most recent update, representing the latest iteration of the pipeline.

# COMMAND ----------

# DBTITLE 1,Get the ID of the most recent pipeline update
# Save the most recent update ID as a Spark configuration setting so it can used in queries.
latest_update_id = spark.sql("SELECT origin.update_id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1").collect()[0].update_id
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# DBTITLE 1,Lineage
# MAGIC %md
# MAGIC Lineage is exposed in the UI as a graph. You can use a query to extract this information to generate reports for compliance or to track data dependencies across an organization. The information related to lineage is stored in the `flow_definition` events and contains the necessary information to infer the relationships between different datasets:

# COMMAND ----------

# DBTITLE 1,Example query for pipeline lineage
# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets FROM event_log_raw WHERE event_type = 'flow_definition' AND origin.update_id = '${latest_update.id}'

# COMMAND ----------

# DBTITLE 1,Data quality
# MAGIC %md
# MAGIC The event log maintains metrics related to data quality. Information related to the data quality checks defined with Delta Live Tables expectations is stored in the `flow_progress` events. The following query extracts the number of passing and failing records for each data quality rule defined for each dataset:

# COMMAND ----------

# DBTITLE 1,Example query for data quality
# MAGIC %sql
# MAGIC SELECT
# MAGIC   substring(message, 34, 26) as table_name,
# MAGIC   SUM(row_expectations.dropped_records) as dropped_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality,
# MAGIC           "array<struct<dropped_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations,
# MAGIC       message
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC     AND
# MAGIC       message not like "%_v2%"
# MAGIC --       AND message LIKE '%Completed a streaming update%'
# MAGIC --       AND origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   message

# COMMAND ----------

_sqldf.display()

# COMMAND ----------

table_dropped_records = _sqldf.collect()

# COMMAND ----------

for table_dropped_record in table_dropped_records:
    if table_dropped_record['dropped_records'] > 0:
        w_log.error(f"[FAIL-DLT-MONITORING]TABLE::{table_dropped_record[0]}|DROPPED_RECORDS::{table_dropped_record[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check DLT Meta Files

# COMMAND ----------

# /dbfs/pipelines/fe789ff7-e646-481c-9ad2-49ad7b814d47/checkpoints/streams_imp_bronze_app_nhn/1/metadata
# /dbfs/pipelines/fe789ff7-e646-481c-9ad2-49ad7b814d47/checkpoints/streams_imp_bronze_app_nhn/1/sources/0/metadata

# (!) current
# /dbfs/pipelines/fe789ff7-e646-481c-9ad2-49ad7b814d47/checkpoints/streams_imp_bronze_app_nhn/1/sources/0/rocksdb/SSTs/088224-4f128b0b-5a76-4f20-aca5-f5661eae152d.sst

# (!) with stat
# /dbfs/pipelines/5102c0b0-20c2-48ff-a477-0ed781c1e94f/checkpoints/tt_streams_bid_bronze_app_nhn/4/sources/0/rocksdb/SSTs/000009-a1e6e379-3956-40e7-996a-49b03c3c7a52.sst

# !cat /dbfs/pipelines/5102c0b0-20c2-48ff-a477-0ed781c1e94f/checkpoints/tt_streams_bid_bronze_app_nhn/4/sources/0/rocksdb/SSTs/000009-a1e6e379-3956-40e7-996a-49b03c3c7a52.sst

# COMMAND ----------

# /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/autoloader/schema_-473140190_/_schemas/1
# /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/autoloader/schema_-768097784_/_schemas/0

# !cat /dbfs/pipelines/d575f43d-4a3e-4b7b-a593-20ac4b561c5d/autoloader/schema_-473140190_/_schemas/1

# COMMAND ----------

# /dbfs/pipelines/5102c0b0-20c2-48ff-a477-0ed781c1e94f/checkpoints/tt_streams_bid_bronze_app_nhn/4/sources/0/rocksdb/logs/000005-61af8783-8d0e-40fd-bc7e-30fc7d514f39.log

# !cat /dbfs/pipelines/5102c0b0-20c2-48ff-a477-0ed781c1e94f/checkpoints/tt_streams_bid_bronze_app_nhn/4/sources/0/rocksdb/logs/000005-61af8783-8d0e-40fd-bc7e-30fc7d514f39.log