# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Propfit Hourly Stat
# MAGIC - Processed Every hour
# MAGIC - Saving Point
# MAGIC   - `cream.propfit_hourly`
# MAGIC - Version
# MAGIC   - Latest `2022-11-21`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Logging

# COMMAND ----------

log4jLogger = spark.sparkContext._jvm.org.apache.log4j
k_log = log4jLogger.LogManager.getLogger("PROPFIT-HOURLY")

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
criteria_time = current_time - timedelta(hours=1)
criteria_date = criteria_time.strftime("%Y-%m-%d")
# criteria_date = '2022-11-04'

# (!) '-' means without zero padding when hour < 10 for HOUR() in SQL
criteria_hour = criteria_time.strftime("%-H")
# criteria_hour = '16'

# COMMAND ----------

criteria_date

# COMMAND ----------

criteria_hour

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Production Query
# MAGIC - Last 1H

# COMMAND ----------

query = f"""
WITH
    bid AS (
            SELECT
                    DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date, -- 입찰 시간
                    actiontime_local AS bidtime, -- raw 입찰 시간
                    BidStream.AdExchangeTypes AS AdExchangeTypes, -- ADX
                    BidStream.ad_adv AS ad_adv, -- 광고주
                    BidStream.ad_imp AS PubId, -- 지면Id
                    BidStream.ad_grp AS ad_grp, -- 광고그룹
                    BidStream.w AS Width, -- 지면크기: 가로
                    BidStream.h AS Height, --지면크기: 세로
                    BidStream.ad_crt AS ad_crt, -- 광고 소재
                    BidStream.bundle_domain AS url, -- 지면도메인
                    BidStream.InventoryIdx AS InventoryIdx, -- 인벤토리IDX
                    BidStream.ad_cam AS ad_cam, -- 캠페인
                    BidStream.UserId AS UserId, -- ADX UserID
                    BidStream.DeviceId AS DeviceId, -- ADID
                    tid -- transaction Id
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
                        FROM ice.streams_bid_bronze_app_nhn
                        WHERE DATE(actiontime_local) = '{criteria_date}'
                        AND HOUR(actiontime_local) = '{criteria_hour}')
            WHERE rn = 1
    ),
    imp AS (
            SELECT
                    DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date, -- 노출 시간
                    actiontime_local AS imptime, -- raw 노출 시간
                    BidStream.AdExchangeTypes AS AdExchangeTypes,
                    BidStream.ad_adv AS ad_adv,
                    BidStream.ad_imp AS PubId,
                    BidStream.ad_grp AS ad_grp,
                    BidStream.w AS Width,
                    BidStream.h AS Height,
                    BidStream.ad_crt AS ad_crt,
                    BidStream.bundle_domain AS url,
                    BidStream.InventoryIdx AS InventoryIdx,
                    BidStream.ad_cam AS ad_cam,
                    BidStream.UserId AS UserId,
                    BidStream.DeviceId AS DeviceId,
                    BidStream.win_price AS Revenue, -- 광고비용
                    tid
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
                        FROM ice.streams_imp_bronze_app_nhn
                        WHERE DATE(actiontime_local) = '{criteria_date}'
                        AND HOUR(actiontime_local) = '{criteria_hour}')
            WHERE rn = 1
    ),
    clk AS (
            SELECT
                    DATE_FORMAT(actiontime_local, 'yyyy-MM-dd HH:00:00') AS Date, -- 클릭 시간
                    actiontime_local AS clktime, -- raw 클릭 시간
                    BidStream.AdExchangeTypes AS AdExchangeTypes,
                    BidStream.ad_adv AS ad_adv,
                    BidStream.ad_imp AS PubId,
                    BidStream.ad_grp AS ad_grp,
                    BidStream.w AS Width,
                    BidStream.h AS Height,
                    BidStream.ad_crt AS ad_crt,
                    BidStream.bundle_domain AS url,
                    BidStream.InventoryIdx AS InventoryIdx,
                    BidStream.ad_cam AS ad_cam,
                    BidStream.UserId AS UserId,
                    BidStream.DeviceId AS DeviceId,
                    tid
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tid ORDER BY actiontime_local) AS rn
                        FROM ice.streams_clk_bronze_app_nhn
                        WHERE DATE(actiontime_local) = '{criteria_date}'
                        AND HOUR(actiontime_local) = '{criteria_hour}')
            WHERE rn = 1
    ),

    -- ############ join_hourly ############
    join_h as (
    SELECT
            bid.Date, -- 입찰 시간
            bid.bidtime, -- raw 입찰 시간
            imp.imptime, -- raw 노출 시간
            clk.clktime, -- raw 클릭 시간
            bid.AdExchangeTypes, -- ADX구분: nhn = 4
            bid.ad_adv, -- 광고주
            bid.PubId, -- 지면Id
            bid.ad_grp, -- 광고그룹
            bid.Width, -- 지면크기: 가로
            bid.Height, --지면크기: 세로
            bid.ad_crt, -- 광고 소재
            bid.url, -- 지면도메인: bundle_domain
            bid.InventoryIdx, -- 인벤토리IDX
            bid.ad_cam, -- 캠페인
            bid.UserId, -- ADX UserID
            bid.DeviceId, -- ADID
            bid.tid,
            imp.Revenue AS Revenue, -- 광고비용
            TIMESTAMPDIFF(SECOND, imptime, clktime) AS ITCT -- impression time to click time
    FROM bid
    LEFT OUTER JOIN imp
        ON bid.Date = imp.Date
        AND bid.ad_adv = imp.ad_adv
            AND bid.AdExchangeTypes = imp.AdExchangeTypes
            AND bid.PubId = imp.PubId
            AND bid.ad_grp = imp.ad_grp
            AND bid.Width = imp.Width
            AND bid.Height = imp.Height
            AND bid.ad_crt = imp.ad_crt
            AND bid.url = imp.url
            AND bid.InventoryIdx = imp.InventoryIdx
            AND bid.ad_cam = imp.ad_cam
            AND bid.UserId = imp.UserId
            AND bid.DeviceId = imp.DeviceId
            AND bid.tid = imp.tid
    LEFT OUTER JOIN clk
        ON bid.Date = clk.Date
        AND bid.ad_adv = clk.ad_adv
            AND bid.AdExchangeTypes = clk.AdExchangeTypes
            AND bid.PubId = clk.PubId
            AND bid.ad_grp = clk.ad_grp
            AND bid.Width = clk.Width
            AND bid.Height = clk.Height
            AND bid.ad_crt = clk.ad_crt
            AND bid.url = clk.url
            AND bid.InventoryIdx = clk.InventoryIdx
            AND bid.ad_cam = clk.ad_cam
            AND bid.UserId = clk.UserId
            AND bid.DeviceId = clk.DeviceId
            AND bid.tid = clk.tid
    WHERE 1=1
    )

-- ############ propfit_hourly ############
SELECT
		Date, -- 입찰 일자
		AdExchangeTypes AS ExchangeTypes, -- ADX구분: nhn = 4
		ad_adv, -- 광고주
		PubId, -- 지면Id
		Width, -- 지면크기: 가로
		Height, -- 지면크기: 세로
		url, -- 지면도메인
		ad_grp, --광고그룹
		InventoryIdx, --인벤토리Idx
		ad_cam, --캠페인
		NVL(COUNT(bidtime), 0) AS bid, -- 입찰 수
		NVL(COUNT(imptime), 0) AS imp, -- 노출 수
		NVL(COUNT(clktime), 0) AS clk, -- 클릭 수
		NVL(SUM(Revenue), 0) AS Revenue -- 광고비용
FROM join_h
-- WHERE DATE(actiontime_local) = '2022-11-04'
-- AND HOUR(actiontime_local) = '15'
GROUP BY PubId, url, ad_cam, ad_grp, Width, Height, InventoryIdx, Date, AdExchangeTypes, ad_adv
"""

# COMMAND ----------

df = spark.sql(query)

# COMMAND ----------

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Delete Previous Data
# MAGIC - Delta

# COMMAND ----------

original_hour = criteria_time.strftime("%H")
# original_hour = '16'

# COMMAND ----------

original_hour

# COMMAND ----------

spark.sql(f"DELETE FROM cream.propfit_hourly WHERE date_format(date, 'yyyy-MM-dd HH:00:00') = '{criteria_date} {original_hour}:00:00' and ExchangeTypes = 4")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save
# MAGIC - Delta
# MAGIC - S3
# MAGIC   - `s3://ptbwa-basic/cream/propfit-hourly/`

# COMMAND ----------

(df.write.format("delta").mode("append").saveAsTable("cream.propfit_hourly"))

# COMMAND ----------

(df.write.mode("overwrite").parquet(f"/mnt/ptbwa-basic/cream/propfit-hourly/{criteria_date}/{original_hour}"))

# COMMAND ----------

df.unpersist()

# COMMAND ----------

k_log.info(f"[PROPFIT-HOURLY]SUCCESS|DATE::{criteria_date}|HOUR::{original_hour}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Test

# COMMAND ----------

# df = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/streams_clk_app_nhn/22/10/31/14/10/streams_clk_app_nhn+3+0000000006.json.gz")

# COMMAND ----------

# s3://ptbwa-basic/topics/bidrequest_app_nhn/22/11/09/05/20/
# df_req = spark.read.json("dbfs:/mnt/ptbwa-basic/topics/bidrequest_app_nhn/22/11/09/05/20/")