# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Propfit 5-Minutes Stat
# MAGIC - Processed Every 5-minutes
# MAGIC - Saving Point
# MAGIC   - `cream.propfit_minutes`
# MAGIC - Version
# MAGIC   - Latest `2022-11-21`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("PROPFIT-MINUTES")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Load Packages

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Criteria Time

# COMMAND ----------

current_time = datetime.now() + timedelta(hours=9)
current_date = current_time.strftime("%Y-%m-%d")
current_hour = current_time.strftime("%-H")
current_minute = current_time.strftime("%-M")
current_minute = int(current_minute)

diff_minute = divmod(current_minute, 5)[1]

# criteria = current - diff
criteria_time = current_time - timedelta(minutes=diff_minute + 5)
criteria_date = criteria_time.strftime("%Y-%m-%d")
criteria_hour = criteria_time.strftime("%-H")
criteria_minute = criteria_time.strftime("%-M")

# COMMAND ----------

current_time

# COMMAND ----------

diff_minute

# COMMAND ----------

criteria_time

# COMMAND ----------

criteria_minute

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Production Query
# MAGIC - Last 5M
# MAGIC   - e.g. `Current Minutes = 33m => Criteria Minutes = 25m (Stat Process on 25m - 29m)`
# MAGIC   - Depends on File Storing-interval on S3

# COMMAND ----------

query = f"""
WITH
		bid AS (
				SELECT
						DATE_FORMAT(CONCAT(DATE(actiontime_local), ' ', HOUR(actiontime_local), ':', FLOOR(MINUTE(actiontime_local) / 5) * 5), 'yyyy-MM-dd HH:mm:ss') AS Date,
						BidStream.AdExchangeTypes AS AdExchangeTypes,
						BidStream.ad_adv AS ad_adv,
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
							WHERE DATE(actiontime_local) = '{criteria_date}'
							AND HOUR(actiontime_local) = '{criteria_hour}'
							AND FLOOR(MINUTE(actiontime_local) / 5) * 5 = '{criteria_minute}')
				WHERE rn = 1
				GROUP BY PubId, url, ad_cam, ad_grp, Width, Height, InventoryIdx, Date, AdExchangeTypes, ad_adv
		),
		imp AS (
				SELECT
						DATE_FORMAT(CONCAT(DATE(actiontime_local), ' ', HOUR(actiontime_local), ':', FLOOR(MINUTE(actiontime_local) / 5) * 5), 'yyyy-MM-dd HH:mm:ss') AS Date,
						BidStream.AdExchangeTypes AS AdExchangeTypes,
						BidStream.ad_adv AS ad_adv,
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
							WHERE DATE(actiontime_local) = '{criteria_date}'
							AND HOUR(actiontime_local) = '{criteria_hour}'
							AND FLOOR(MINUTE(actiontime_local) / 5) * 5 = '{criteria_minute}')
				WHERE rn = 1
				GROUP BY PubId, url, ad_cam, ad_grp, Width, Height, InventoryIdx, Date, AdExchangeTypes, ad_adv
		),
		clk AS (
				SELECT
						DATE_FORMAT(CONCAT(DATE(actiontime_local), ' ', HOUR(actiontime_local), ':', FLOOR(MINUTE(actiontime_local) / 5) * 5), 'yyyy-MM-dd HH:mm:ss') AS Date,
						BidStream.AdExchangeTypes AS AdExchangeTypes,
						BidStream.ad_adv AS ad_adv,
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
							WHERE DATE(actiontime_local) = '{criteria_date}'
							AND HOUR(actiontime_local) = '{criteria_hour}'
							AND FLOOR(MINUTE(actiontime_local) / 5) * 5 = '{criteria_minute}')
				WHERE rn = 1
				GROUP BY PubId, url, ad_cam, ad_grp, Width, Height, InventoryIdx, Date, AdExchangeTypes, ad_adv
		)

-- ############ cream.propfit_minutes ############
-- 2022-11-30 bid; 총 1개 컬럼 추가
SELECT
		cast(bid.Date as TIMESTAMP), -- 입찰 시간
		bid.AdExchangeTypes AS ExchangeTypes, -- ADX구분: nhn = 4
		bid.ad_adv, -- 광고주
		bid.PubId, -- 지면Id
		bid.ad_grp, -- 광고그룹
		bid.Width, -- 지면크기: 가로
		bid.Height, --지면크기: 세로
		bid.url, -- 지면도메인: bundle_domain
		bid.InventoryIdx, -- 인벤토리IDX
		cast(bid.ad_cam AS INTEGER), -- 캠페인
		NVL(bid.bid, 0) AS Bid,
		NVL(imp.imp, 0) AS Imps,
		NVL(clk.clk, 0) AS Click,
		NVL(imp.Revenue, 0) AS Revenue
FROM bid
LEFT OUTER JOIN imp
    ON bid.Date = imp.Date
    AND bid.AdExchangeTypes = imp.AdExchangeTypes
    AND bid.ad_adv = imp.ad_adv
    AND bid.PubId = imp.PubId
    AND bid.Width = imp.Width
    AND bid.Height = imp.Height
    AND bid.url = imp.url
    AND bid.ad_grp = imp.ad_grp
    AND bid.InventoryIdx = imp.InventoryIdx
    AND bid.ad_cam = imp.ad_cam
LEFT OUTER JOIN clk
    ON bid.Date = clk.Date
    AND bid.AdExchangeTypes = imp.AdExchangeTypes
    AND bid.ad_adv = clk.ad_adv
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

df = spark.sql(query)

# COMMAND ----------

df.cache()

# COMMAND ----------

# df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Delete Previous Data
# MAGIC - Delta

# COMMAND ----------

original_hour = criteria_time.strftime("%H")
original_minute = '0' + criteria_minute if int(criteria_minute) < 10 else criteria_minute

# COMMAND ----------

original_hour

# COMMAND ----------

original_minute

# COMMAND ----------

spark.sql(f"DELETE FROM cream.propfit_minutes WHERE date_format(date, 'yyyy-MM-dd HH:mm:00') = '{criteria_date} {original_hour}:{original_minute}:00' and ExchangeTypes = 4")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save
# MAGIC - Delta
# MAGIC - S3
# MAGIC   - `s3://ptbwa-basic/cream/propfit-minutes/`

# COMMAND ----------

(df.write.format("delta").mode("append").saveAsTable("cream.propfit_minutes"))

# COMMAND ----------

#df.write.mode("overwrite").parquet(f"/mnt/ptbwa-basic/cream/propfit-minutes/{criteria_date}/{original_hour}/{original_minute}")

# COMMAND ----------

df.unpersist()

# COMMAND ----------

k_log.info(f"[PROPFIT-MINUTES]SUCCESS|DATE::{criteria_date}|HOUR::{original_hour}|MINUTE::{original_minute}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

# df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/22/10/31/14/10/streams_clk_app_nhn+3+0000000006.json.gz")

# COMMAND ----------

# s3://ptbwa-basic/topics/bidrequest_app_nhn/22/11/09/05/20/
# df_req = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/bidrequest_app_nhn/22/11/09/05/20/")