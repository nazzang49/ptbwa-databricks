# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### DeepDive into Spark Partition

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Data
# MAGIC - Spark SQL
# MAGIC   - Auto Report Raw
# MAGIC   - Auto Report Stat

# COMMAND ----------

# df = spark.sql("select * from auto_report.ad_series_deliver_stats")
df = spark.sql("select * from auto_report.airb_funble_mmp_raw")
# df = spark.sql("select * from auto_report.tt_airb_funble_mmp_raw")

# COMMAND ----------

df = df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Left Join

# COMMAND ----------

query = """
WITH
		bid AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS bid
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_bid_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		),
		imp AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS imp,
						SUM(BidStream.win_price) AS Revenue
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_imp_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		),
		clk AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS clk
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_clk_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		)

SELECT
		'4' AS ExchangeTypes,
		bid.Date,
		bid.PubId,
		bid.Width,
		bid.Height,
		bid.url,
		bid.ad_grp,
		bid.InventoryIdx,
		bid.ad_cam,
		NVL(bid.bid, 0) AS bid,
		NVL(imp.imp, 0) AS imp,
		NVL(clk.clk, 0) AS clk,
		NVL(imp.Revenue, 0) AS Revenue
FROM bid
LEFT OUTER JOIN imp
    ON bid.Date = imp.Date
    AND bid.PubId = imp.PubId 
    AND bid.Width = imp.Width 
    AND bid.Height = imp.Height 
    AND bid.url = imp.url 
    AND bid.ad_grp = imp.ad_grp 
    AND bid.InventoryIdx = imp.InventoryIdx 
    AND bid.ad_cam = imp.ad_cam
LEFT OUTER JOIN clk
    ON bid.Date = clk.Date
    AND bid.PubId = clk.PubId 
    AND bid.Width = clk.Width 
    AND bid.Height = clk.Height 
    AND bid.url = clk.url 
    AND bid.ad_grp = clk.ad_grp 
    AND bid.InventoryIdx = clk.InventoryIdx 
    AND bid.ad_cam = clk.ad_cam
WHERE 1=1
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Right Join

# COMMAND ----------

query = """
WITH
		bid AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS bid
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_bid_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		),
		imp AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS imp,
						SUM(BidStream.win_price) AS Revenue
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_imp_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		),
		clk AS (
				SELECT
						DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date,
						BidStream.ad_imp AS PubId,
						BidStream.ad_grp AS ad_grp,
						BidStream.w AS Width,
						BidStream.h AS Height,
						BidStream.bundle_domain AS url,
						BidStream.InventoryIdx AS InventoryIdx,
						BidStream.ad_cam AS ad_cam,
						COUNT(*) AS clk
				FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
							FROM ice.streams_clk_bronze_app_nhn
							WHERE DATE(actiontime_local) = '2022-11-04'
							AND HOUR(actiontime_local) = '15')
				WHERE rn = 1
				GROUP BY Date, PubId, Width, Height, url, ad_grp, InventoryIdx, ad_cam
		)

SELECT
		'4' AS ExchangeTypes,
		clk.Date,
		clk.PubId,
		clk.Width,
		clk.Height,
		clk.url,
		clk.ad_grp,
		clk.InventoryIdx,
		clk.ad_cam,
		NVL(bid.bid, 0) AS bid,
		NVL(imp.imp, 0) AS imp,
		NVL(clk.clk, 0) AS clk,
		NVL(imp.Revenue, 0) AS Revenue
FROM bid
RIGHT OUTER JOIN imp
    ON bid.Date = imp.Date
    AND bid.PubId = imp.PubId 
    AND bid.Width = imp.Width 
    AND bid.Height = imp.Height 
    AND bid.url = imp.url 
    AND bid.ad_grp = imp.ad_grp 
    AND bid.InventoryIdx = imp.InventoryIdx 
    AND bid.ad_cam = imp.ad_cam
RIGHT OUTER JOIN clk
    ON bid.Date = clk.Date
    AND bid.PubId = clk.PubId 
    AND bid.Width = clk.Width 
    AND bid.Height = clk.Height 
    AND bid.url = clk.url 
    AND bid.ad_grp = clk.ad_grp 
    AND bid.InventoryIdx = clk.InventoryIdx 
    AND bid.ad_cam = clk.ad_cam
WHERE 1=1
"""

# COMMAND ----------

df_hourly = spark.sql(query)

# COMMAND ----------

df_hourly.display()

# COMMAND ----------

df_hourly.rdd.getNumPartitions()

# COMMAND ----------

df_hourly.repartition(16)

# COMMAND ----------

df_hourly.repartition(1).write.mode("overwrite").parquet("dbfs:/mnt/ptbwa-basic/test/test1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Diff with or without Repartition
# MAGIC - based on `auto_report.ad_series_deliver_stats` 
# MAGIC   - 8M Rows (2022-11-14)
# MAGIC   - 17-partitions :: 170MB / 17 Files / Skew / 17 Call Counts
# MAGIC   - 1-partition :: 170MB / 1 File / Non-Skew / 1 Call Count
# MAGIC - (!) Repartition Neccessary when Write
# MAGIC   - 파일 갯수 감소
# MAGIC   - 파일 크기 증가

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.repartition(16).write.mode("overwrite").parquet("dbfs:/mnt/ptbwa-basic/test/test2")
# df.write.mode("overwrite").parquet("dbfs:/mnt/ptbwa-basic/test/test2")

# COMMAND ----------

df.groupBy(["EventCategory"]).sum("ScreenWidth").display()

# COMMAND ----------

df.groupBy(["EventCategory"]).sum("ScreenWidth").write.mode("overwrite").parquet("dbfs:/mnt/ptbwa-basic/test/test3")

# COMMAND ----------

df.filter("EventCategory is not null").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Analyze Spark SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC EXPLAIN EXTENDED SELECT /*+ REPARTITION(3) */ * FROM auto_report.ad_series_cohort_stats;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Details about Partition in Spark DF

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

df = spark.sql("select * from ice.streams_clk_bronze_app_nhn WHERE DATE(actiontime_local) = '2023-02-01' AND HOUR(actiontime_local) = 13 AND FLOOR(MINUTE(actiontime_local) / 5) * 5 = 30")

# COMMAND ----------

check_details(df.repartition(12))

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df.rdd.getStorageLevel()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Spark Configuration
# MAGIC - Check Current Setting
# MAGIC - Change Setting

# COMMAND ----------

# (!) caution :: important for enhancing spark performance
# spark.conf.set("spark.sql.Shuffle.Partitions", 10)

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Concepts related with Spark Performance
# MAGIC - Spill
# MAGIC   - Serialization when Stored
# MAGIC   - Evoke Additional Time Spent for Deserializing when operated
# MAGIC - Partition
# MAGIC   - Input
# MAGIC   - Output
# MAGIC     - Repartition
# MAGIC     - Coalesce
# MAGIC   - Shuffle
# MAGIC     - Critical Property for Spark Performance

# COMMAND ----------

# load json file e.g. s3://ptbwa-basic/topics/bidrequest_app_nhn/22/11/02/23/35/
df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/bidrequest_app_nhn/22/11/02/23/35/")

# COMMAND ----------

df.count()

# COMMAND ----------

# load json file e.g. s3://ptbwa-basic/test/test2/
df = spark.read.parquet("dbfs:/mnt/ptbwa-basic/test/test2/")

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter("EventCategory is not null").count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Auto Optimize
# MAGIC - with Z-Ordering

# COMMAND ----------

df = spark.sql("select * from auto_report.gad_kcar_general_stats")

# COMMAND ----------

df.write.mode("append").saveAsTable("auto_report.ttt_gad_kcar_general_stats")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Check Shuffle

# COMMAND ----------

# 10MB in default
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

