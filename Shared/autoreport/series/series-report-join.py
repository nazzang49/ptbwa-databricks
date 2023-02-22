# Databricks notebook source
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("SERIES")

# COMMAND ----------

from datetime import datetime, timedelta
from pytz import timezone

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get Params
# MAGIC - env
# MAGIC   - dev
# MAGIC   - prod

# COMMAND ----------

env = "prod"
# env = "dev"

# COMMAND ----------

try:
    # (!) LIVE
    env = dbutils.widgets.get("env")
    
    # (!) TEST
#     env = "dev"
except Exception as e:
    log.info("[FAIL-GET-ENV-PARAMS]THIS-IS-PROD-ACTION(DEFAULT)")

# COMMAND ----------

database = "tt_auto_report" if env == "dev" else "auto_report"

# COMMAND ----------

database

# COMMAND ----------

KST = timezone('Asia/Seoul')
date = datetime.strftime((datetime.now(KST) - timedelta(5)), "%Y-%m-%d")
date

# COMMAND ----------

today = datetime.strftime((datetime.now(KST)), "%Y-%m-%d")
today

# COMMAND ----------

try:
    df = spark.sql(f"DELETE FROM {database}.series_report_d WHERE date >= '{date}' OR Network_sub IS NULL")
    log.info(f"[SUCCESS-DELETE-SERIES-REPORT-D]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-DELETE-SERIES-REPORT-D]{e}")
    raise e

# COMMAND ----------

############################################################ [ Google Ads ACI ] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network AS Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, b.creativeId, a.creativeName, a.adgroupName AS title, a.OS_Name,
                        a.impressions, a.clicks, NULL AS engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, NULL AS videoplay, NULL AS videoP25, NULL AS videoP50, NULL AS videoP75, NULL AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            Network,
                            'android' AS OS_Name,
                            campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            creativeName,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(installs) AS installs,
                            SUM(spend)*1.12/1000000 AS spend
                        FROM (
                            SELECT
                                segmentsDate AS Date,
                                customerName,
                                CASE customerName
                                    WHEN 'SERIES' THEN 'Google Ads ACI'
                                    WHEN 'SERIES Re-engagement' THEN 'Google Ads ACE' END AS Network,
                                campaignId,
                                campaignName,
                                adgroupId,
                                adgroupName,
                                CASE networkType
                                    WHEN '2' THEN 'Search'
                                    WHEN '3' THEN 'Search'
                                    WHEN '4' THEN 'Display'
                                    WHEN '5' THEN 'Search'
                                    WHEN '6' THEN 'Youtube'
                                    ELSE 'Unknown' END AS creativeName,
                                NVL(impressions, 0) AS impressions,
                                NVL(interactions, 0) AS clicks,
                                NVL(conversions, 0) AS installs,
                                NVL(costMicros, 0) AS spend
                            FROM {database}.gad_series_adgroup_stats
                            WHERE segmentsDate >= '{date}'
                        )
                        GROUP BY
                            Date,
                            Network, OS_Name,
                            campaignId, campaignName, adgroupId, adgroupName, creativeName
                        HAVING
                            Network = 'Google Ads ACI'
                        ) a
                        
                        LEFT JOIN 
                            {database}.ad_series_deliver_stats b
                        ON a.Date = b.Date AND a.Network = b.Network_sub AND a.CampaignId = b.CampaignId AND a.AdgroupId = b.AdgroupId AND a.creativeName = b.CreativeName
                            AND b.Date >= '{date}'
                            AND b.Network_sub = 'Google Ads ACI'
                        
                        LEFT JOIN
                            {database}.ad_series_cohort_stats c
                        ON c.Date = a.Date AND c.Network_sub = a.Network AND c.CampaignId = a.CampaignId AND c.AdgroupId = a.AdgroupId AND c.creativeName = a.CreativeName
                            AND c.Date >= '{date}'
                            AND c.cohort = 'ua'
                            AND c.Network_sub = 'Google Ads ACI'

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON a.Network = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-GAD-ACI-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-GAD-ACI-REPORT]{e}")
    raise e

# COMMAND ----------

############################################################ [ Google Ads NOT APP CAMPAIGN] ############################################################
# try:
#     insert_query = f"""
#             INSERT INTO {database}.series_report_d
#                     SELECT DISTINCT
#                         a.Date, a.week, a.dayoftheweek, a.Network AS Network_sub, e.`네트워크명` AS campaignGoal,
#                         a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, b.creativeId, a.creativeName, a.adgroupName AS title, a.OS_Name,
#                         a.impressions, a.clicks, NULL AS engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
#                         b.installs, b.reattributions,
#                         c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
#                         b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
#                     FROM (
#                         SELECT
#                             Date,
#                             EXTRACT(week FROM Date) AS week,
#                             DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
#                             Network,
#                             'android' AS OS_Name,
#                             campaignId,
#                             campaignName,
#                             adgroupId,
#                             adgroupName,
#                             creativeName,
#                             SUM(impressions) AS impressions,
#                             SUM(clicks) AS clicks,
#                             SUM(installs) AS installs,
#                             SUM(spend)*1.12/1000000 AS spend,
#                             SUM(videoP25) AS videoP25,
#                             SUM(videoP50) AS videoP50,
#                             SUM(videoP75) AS videoP75,
#                             SUM(videoP100) AS videoP100,
#                             SUM(videoplay) AS videoplay
#                         FROM (
#                             SELECT
#                                 segmentsDate AS Date,
#                                 customerName,
# --                              m.campaign_network AS Network,
#                                 campaignId,
#                                 campaignName,
#                                 adgroupId,
#                                 adgroupName,
#                                 adId AS creativeId,
#                                 adName AS creativeName,
#                                 NVL(metricsImpressions, 0) AS impressions,
#                                 NVL(metricsClicks, 0) AS clicks,
#                                 NVL(metricsAllConversions, 0) AS installs, --상호작용수
#                                 NVL(metricsCostMicros, 0) AS spend,
#                                 NVL(metricsVideoQuartileP25Rate, 0) AS videoP25,
#                                 NVL(metricsVideoQuartileP50Rate, 0) AS videoP50,
#                                 NVL(metricsVideoQuartileP75Rate, 0) AS videoP75,
#                                 NVL(metricsVideoQuartileP100Rate, 0) AS videoP100,
#                                 NVL(metricsVideoViews, 0) AS videoplay
#                             FROM {database}.gad_series_ad_stats g
#                             LEFT JOIN
#                                 {database}.ad_series_map_cp_cpsub_ing m
#                             ON g.campaignName = m.campaign
#                             WHERE segmentsDate >= '{date}'
#                         )
#                         GROUP BY
#                             Date,
#                             Network, OS_Name,
#                             campaignId, campaignName, adgroupId, adgroupName, creativeName
#                         ) a

#                         LEFT JOIN 
#                             {database}.ad_series_deliver_stats b
#                         ON a.Date = b.Date AND a.Network = b.Network_sub AND a.CampaignId = b.CampaignId AND a.AdgroupId = b.AdgroupId AND a.creativeName = b.CreativeName

#                         LEFT JOIN
#                             {database}.ad_series_cohort_stats c
#                         ON c.Date = a.Date AND c.Network_sub = a.Network AND c.CampaignId = a.CampaignId AND c.AdgroupId = a.AdgroupId AND c.creativeName = a.CreativeName
#                             AND c.cohort = 'all'

#                         LEFT JOIN
#                             {database}.ad_series_map_media_network e
#                         ON a.Network = e.`매체`
#         """            
#     df = spark.sql(insert_query)
#     log.info(f"[SUCCESS-INSERT-SERIES-GAD-NOTAPP-REPORT]")
# except Exception as e:
#     print(e)
#     log.error(f"[FAIL-INSERT-SERIES-GAD-NOTAPP-REPORT]{e}")
#     raise e

# COMMAND ----------

############################################################ [ Google Ads ACE ] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network AS Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, b.creativeId, a.creativeName, a.adgroupName AS title, a.OS_Name,
                        a.impressions, a.clicks, NULL AS engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, NULL AS videoplay, NULL AS videoP25, NULL AS videoP50, NULL AS videoP75, NULL AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            Network,
                            'android' AS OS_Name,
                            campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            creativeName,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(installs) AS installs,
                            SUM(spend)*1.12/1000000 AS spend
                        FROM (
                            SELECT
                                segmentsDate AS Date,
                                customerName,
                                CASE customerName
                                    WHEN 'SERIES' THEN 'Google Ads ACI'
                                    WHEN 'SERIES Re-engagement' THEN 'Google Ads ACE' END AS Network,
                                campaignId,
                                campaignName,
                                adgroupId,
                                adgroupName,
                                CASE networkType
                                    WHEN '2' THEN 'Search'
                                    WHEN '3' THEN 'Search'
                                    WHEN '4' THEN 'Display'
                                    WHEN '5' THEN 'Search'
                                    WHEN '6' THEN 'Youtube'
                                    ELSE 'Unknown' END AS creativeName,
                                NVL(impressions, 0) AS impressions,
                                NVL(interactions, 0) AS clicks,
                                NVL(conversions, 0) AS installs,
                                NVL(costMicros, 0) AS spend
                            FROM {database}.gad_series_adgroup_stats
                            WHERE segmentsDate >= '{date}'
                        )
                        GROUP BY
                            Date,
                            Network, OS_Name,
                            campaignId, campaignName, adgroupId, adgroupName, creativeName
                        HAVING
                            Network = 'Google Ads ACE'
                        ) a
                        
                        LEFT JOIN 
                            {database}.ad_series_deliver_stats b
                        ON a.Date = b.Date AND a.Network = b.Network_sub AND a.CampaignId = b.CampaignId AND a.AdgroupId = b.AdgroupId AND a.creativeName = b.CreativeName
                            AND b.Date >= '{date}'
                            AND b.Network_sub = 'Google Ads ACE'
                        
                        LEFT JOIN
                            {database}.ad_series_cohort_stats c
                        ON c.Date = a.Date AND c.Network_sub = a.Network AND c.CampaignId = a.CampaignId AND c.AdgroupId = a.AdgroupId AND c.creativeName = a.CreativeName
                            AND c.Date >= '{date}'
                            AND c.cohort = 're'
                            AND c.Network_sub = 'Google Ads ACE'

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON a.Network = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-GAD-ACE-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-GAD-ACE-REPORT]{e}")
    raise e

# COMMAND ----------

############################################################ [ Facebook ] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            OS_Name,
                            Network_sub,
                            campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            creativeId,
                            creativeName,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(engagements) AS engagements,
                            SUM(spend)*1.12 AS spend,
                            SUM(videoplay) AS videoplay,
                            SUM(videoP25) AS videoP25,
                            SUM(videoP50) AS videoP50,
                            SUM(videoP75) AS videoP75,
                            SUM(videoP100) AS videoP100
                        FROM (
                            SELECT
                                dateStart AS Date,
                                CASE
                                    WHEN impressionDevice LIKE 'android%' THEN 'android'
                                    WHEN impressionDevice IN ('iphone', 'ipad', 'ipod') THEN 'ios'
                                    ELSE 'unknown' END AS OS_Name,
                                Network_sub,
                                campaignId,
                                campaignName,
                                adsetId AS adgroupId,
                                adsetName AS adgroupName,
                                adId AS creativeId,
                                adName AS creativeName,
                                NVL(impressions, 0) AS impressions,
                                NVL(linkClick, 0) AS clicks,
                                NVL(postEngagement, 0) AS engagements,
                                NVL(spend, 0) AS spend,
                                NVL(videoPlayActions, 0) AS videoplay,
                                NVL(videoP25WatchedActions, 0) AS videoP25,
                                NVL(videoP50WatchedActions, 0) AS videoP50,
                                NVL(videoP75WatchedActions, 0) AS videoP75,
                                NVL(videoP100WatchedActions, 0) AS videoP100
                            FROM {database}.fb_series_ad_stats
                            WHERE dateStart >= '{date}'
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) NOT LIKE '%_traffic%'
                        )
                        GROUP BY
                                Date, Network_sub, OS_Name,
                                campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
                    ) a

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                            FROM {database}.ad_series_deliver_stats
                            WHERE Date >= '{date}'
                            AND (Network_sub LIKE '%Facebook%' OR Network_sub LIKE '%Meta%')
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) NOT LIKE '%_traffic%'
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
                        ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
                                SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                                SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                            FROM {database}.ad_series_cohort_stats
                            WHERE Date >= '{date}'
                            AND cohort = 'ua'
                            AND (Network_sub LIKE '%Facebook%' OR Network_sub LIKE '%Meta%')
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) NOT LIKE '%_traffic%'
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
                        ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

                        LEFT JOIN
                            {database}.ad_series_map_creative_title d
                        ON LOWER(a.creativeName) = LOWER(d.Creative)

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON REPLACE(a.Network_sub, 'Facebook', 'Meta_Installs') = e.`매체`

            UNION ALL
            
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            OS_Name,
                            Network_sub,
                            campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            creativeId,
                            creativeName,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(engagements) AS engagements,
                            SUM(spend)*1.12 AS spend,
                            SUM(videoplay) AS videoplay,
                            SUM(videoP25) AS videoP25,
                            SUM(videoP50) AS videoP50,
                            SUM(videoP75) AS videoP75,
                            SUM(videoP100) AS videoP100
                        FROM (
                            SELECT
                                dateStart AS Date,
                                CASE
                                    WHEN impressionDevice LIKE 'android%' THEN 'android'
                                    WHEN impressionDevice IN ('iphone', 'ipad', 'ipod') THEN 'ios'
                                    ELSE 'unknown' END AS OS_Name,
                                Network_sub,
                                campaignId,
                                campaignName,
                                adsetId AS adgroupId,
                                adsetName AS adgroupName,
                                adId AS creativeId,
                                adName AS creativeName,
                                NVL(impressions, 0) AS impressions,
                                NVL(linkClick, 0) AS clicks,
                                NVL(postEngagement, 0) AS engagements,
                                NVL(spend, 0) AS spend,
                                NVL(videoPlayActions, 0) AS videoplay,
                                NVL(videoP25WatchedActions, 0) AS videoP25,
                                NVL(videoP50WatchedActions, 0) AS videoP50,
                                NVL(videoP75WatchedActions, 0) AS videoP75,
                                NVL(videoP100WatchedActions, 0) AS videoP100
                            FROM {database}.fb_series_ad_stats
                            WHERE dateStart >= '{date}'
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) LIKE '%_traffic%'
                        )
                        GROUP BY
                            Date, Network_sub, OS_Name,
                            campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
                        ) a

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                            FROM {database}.ad_series_deliver_stats
                            WHERE Date >= '{date}'
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) LIKE '%_traffic%'
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
                        ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
                                SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                                SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                            FROM {database}.ad_series_cohort_stats
                            WHERE Date >= '{date}'
                            AND cohort = 'all'
                            AND (Network_sub <> 'Facebook RE' AND Network_sub <> 'Meta RE')
                            AND LOWER(campaignName) LIKE '%_traffic%'
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
                        ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

                        LEFT JOIN
                            {database}.ad_series_map_creative_title d
                        ON LOWER(a.creativeName) = LOWER(d.Creative)

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON REPLACE(REPLACE(a.Network_sub, 'Facebook', 'Meta_Installs'), 'Facebook_SNS', 'Meta_Traffic') = e.`매체`

            UNION ALL
            
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            OS_Name,
                            Network_sub,
                            campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            creativeId,
                            creativeName,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(engagements) AS engagements,
                            SUM(spend)*1.12 AS spend,
                            SUM(videoplay) AS videoplay,
                            SUM(videoP25) AS videoP25,
                            SUM(videoP50) AS videoP50,
                            SUM(videoP75) AS videoP75,
                            SUM(videoP100) AS videoP100
                        FROM (
                            SELECT
                                dateStart AS Date,
                                CASE
                                    WHEN impressionDevice LIKE 'android%' THEN 'android'
                                    WHEN impressionDevice IN ('iphone', 'ipad', 'ipod') THEN 'ios'
                                    ELSE 'unknown' END AS OS_Name,
                                Network_sub,
                                campaignId,
                                campaignName,
                                adsetId AS adgroupId,
                                adsetName AS adgroupName,
                                adId AS creativeId,
                                adName AS creativeName,
                                NVL(impressions, 0) AS impressions,
                                NVL(linkClick, 0) AS clicks,
                                NVL(postEngagement, 0) AS engagements,
                                NVL(spend, 0) AS spend,
                                NVL(videoPlayActions, 0) AS videoplay,
                                NVL(videoP25WatchedActions, 0) AS videoP25,
                                NVL(videoP50WatchedActions, 0) AS videoP50,
                                NVL(videoP75WatchedActions, 0) AS videoP75,
                                NVL(videoP100WatchedActions, 0) AS videoP100
                            FROM {database}.fb_series_ad_stats
                            WHERE dateStart >= '{date}'
                            AND Network_sub = 'Facebook RE'
                        )
                        GROUP BY
                            Date, Network_sub, OS_Name,
                            campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
                        ) a

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                            FROM {database}.ad_series_deliver_stats
                            WHERE Date >= '{date}'
                            AND (Network_sub = 'Facebook RE' OR Network_sub = 'Meta_RE')
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
                        ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

                        LEFT JOIN (
                            SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
                                SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                                SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                            FROM {database}.ad_series_cohort_stats
                            WHERE Date >= '{date}'
                            AND cohort = 're'
                            AND (Network_sub = 'Facebook RE' OR Network_sub = 'Meta_RE')
                            GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
                        ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

                        LEFT JOIN
                            {database}.ad_series_map_creative_title d
                        ON LOWER(a.creativeName) = LOWER(d.Creative)

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON REPLACE(a.Network_sub, 'Facebook RE', 'Meta_RE') = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-FB-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-FB-REPORT]{e}")
    raise e

# COMMAND ----------

# ############################################################ [ Naver GFA ] ############################################################
# try:
#     insert_query = f"""
#             INSERT INTO {database}.series_report_d
#                     SELECT DISTINCT
#                         a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
#                         a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
#                         a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
#                         b.installs, b.reattributions,
#                         c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
#                         b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
#                     FROM (
#                         SELECT
#                             Date,
#                             EXTRACT(week FROM Date) AS week,
#                             DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
#                             OS_Name,
#                             Network_sub,
#                             campaignId,
#                             campaignName,
#                             adgroupId,
#                             adgroupName,
#                             creativeId,
#                             creativeName,
#                             SUM(impressions) AS impressions,
#                             SUM(clicks) AS clicks,
#                             SUM(engagements) AS engagements,
#                             SUM(spend)*1.12 AS spend,
#                             SUM(videoplay) AS videoplay,
#                             SUM(videoP25) AS videoP25,
#                             SUM(videoP50) AS videoP50,
#                             SUM(videoP75) AS videoP75,
#                             SUM(videoP100) AS videoP100
#                         FROM (
#                             SELECT
#                                 Date,
#                                 CASE
#                                     WHEN LOWER(osName) LIKE '%android%' THEN 'android'
#                                     WHEN LOWER(osName) LIKE '%ios%' THEN 'ios'
#                                     ELSE 'unknown' END AS OS_Name,
#                                 'NAVER_GFA' AS Network_sub,
#                                 campaignId,
#                                 campaignName,
#                                 adgroupId,
#                                 adgroupName,
#                                 creativeId,
#                                 creativeName,
#                                 NVL(impressions, 0) AS impressions,
#                                 NVL(clicks, 0) AS clicks,
#                                 NVL(reach, 0) AS engagements,
#                                 NVL(totalCurrency, 0) AS spend,
#                                 NVL(videoplay, 0) AS videoplay,
#                                 NVL(videoP25, 0) AS videoP25,
#                                 NVL(videoP50, 0) AS videoP50,
#                                 NVL(videoP75, 0) AS videoP75,
#                                 NVL(videoP100, 0) AS videoP100
#                             FROM {database}.gfa_series_stats
#                             WHERE Date >= '{date}'
#                             AND LOWER(campaignName) NOT LIKE '%_traffic%'
#                           )
#                         GROUP BY
#                             Date, Network_sub, OS_Name,
#                             campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
#                         ) a

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
#                         FROM {database}.ad_series_deliver_stats
#                         WHERE Date >= '{date}'
#                         AND Network_sub = 'NAVER_GFA'
#                         AND LOWER(campaignName) NOT LIKE '%_traffic%'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
#                     ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
#                             SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
#                             SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
#                         FROM {database}.ad_series_cohort_stats
#                         WHERE Date >= '{date}'
#                         AND cohort = 'ua'
#                         AND Network_sub = 'NAVER_GFA'
#                         AND LOWER(campaignName) NOT LIKE '%_traffic%'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
#                     ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

#                     LEFT JOIN
#                         {database}.ad_series_map_creative_title d
#                     ON LOWER(a.creativeName) = LOWER(d.Creative)

#                     LEFT JOIN
#                         {database}.ad_series_map_media_network e
#                     ON a.Network_sub = e.`매체`

#                 UNION ALL

#                     SELECT DISTINCT
#                         a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
#                         a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
#                         a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
#                         b.installs, b.reattributions,
#                         c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
#                         b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
#                     FROM (
#                         SELECT
#                             Date,
#                             EXTRACT(week FROM Date) AS week,
#                             DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
#                             OS_Name,
#                             Network_sub,
#                             campaignId,
#                             campaignName,
#                             adgroupId,
#                             adgroupName,
#                             creativeId,
#                             creativeName,
#                             SUM(impressions) AS impressions,
#                             SUM(clicks) AS clicks,
#                             SUM(engagements) AS engagements,
#                             SUM(spend)*1.12 AS spend,
#                             SUM(videoplay) AS videoplay,
#                             SUM(videoP25) AS videoP25,
#                             SUM(videoP50) AS videoP50,
#                             SUM(videoP75) AS videoP75,
#                             SUM(videoP100) AS videoP100
#                         FROM (
#                             SELECT
#                                 Date,
#                                 CASE
#                                     WHEN LOWER(osName) LIKE '%android%' THEN 'android'
#                                     WHEN LOWER(osName) LIKE '%ios%' THEN 'ios'
#                                     ELSE 'unknown' END AS OS_Name,
#                                 'NAVER_GFA' AS Network_sub,
#                                 campaignId,
#                                 campaignName,
#                                 adgroupId,
#                                 adgroupName,
#                                 creativeId,
#                                 creativeName,
#                                 NVL(impressions, 0) AS impressions,
#                                 NVL(clicks, 0) AS clicks,
#                                 NVL(reach, 0) AS engagements,
#                                 NVL(totalCurrency, 0) AS spend,
#                                 NVL(videoplay, 0) AS videoplay,
#                                 NVL(videoP25, 0) AS videoP25,
#                                 NVL(videoP50, 0) AS videoP50,
#                                 NVL(videoP75, 0) AS videoP75,
#                                 NVL(videoP100, 0) AS videoP100
#                             FROM {database}.gfa_series_stats
#                             WHERE Date >= '{date}'
#                             AND LOWER(campaignName) LIKE '%_traffic%'
#                             )
#                         GROUP BY
#                             Date, Network_sub, OS_Name,
#                             campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
#                         ) a

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
#                         FROM {database}.ad_series_deliver_stats
#                         WHERE Date >= '{date}'
#                         AND Network_sub = 'NAVER_GFA'
#                         AND LOWER(campaignName) LIKE '%_traffic%'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
#                     ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
#                             SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
#                             SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
#                         FROM {database}.ad_series_cohort_stats
#                         WHERE Date >= '{date}'
#                         AND cohort = 'all'
#                         AND Network_sub = 'NAVER_GFA'
#                         AND LOWER(campaignName) LIKE '%_traffic%'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
#                     ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

#                     LEFT JOIN
#                         {database}.ad_series_map_creative_title d
#                     ON LOWER(a.creativeName) = LOWER(d.Creative)

#                     LEFT JOIN
#                         {database}.ad_series_map_media_network e
#                     ON a.Network_sub = e.`매체`

#                 UNION ALL

#                     SELECT DISTINCT
#                         a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
#                         a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
#                         a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
#                         b.installs, b.reattributions,
#                         c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
#                         b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
#                     FROM (
#                         SELECT
#                             Date,
#                             EXTRACT(week FROM Date) AS week,
#                             DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
#                             OS_Name,
#                             Network_sub,
#                             campaignId,
#                             campaignName,
#                             adgroupId,
#                             adgroupName,
#                             creativeId,
#                             creativeName,
#                             SUM(impressions) AS impressions,
#                             SUM(clicks) AS clicks,
#                             SUM(engagements) AS engagements,
#                             SUM(spend)*1.12 AS spend,
#                             SUM(videoplay) AS videoplay,
#                             SUM(videoP25) AS videoP25,
#                             SUM(videoP50) AS videoP50,
#                             SUM(videoP75) AS videoP75,
#                             SUM(videoP100) AS videoP100
#                         FROM (
#                             SELECT
#                                 Date,
#                                 CASE
#                                     WHEN LOWER(osName) LIKE '%android%' THEN 'android'
#                                     WHEN LOWER(osName) LIKE '%ios%' THEN 'ios'
#                                     ELSE 'unknown' END AS OS_Name,
#                                 'NAVER_GFA' AS Network_sub,
#                                 campaignId,
#                                 campaignName,
#                                 adgroupId,
#                                 adgroupName,
#                                 creativeId,
#                                 creativeName,
#                                 NVL(impressions, 0) AS impressions,
#                                 NVL(clicks, 0) AS clicks,
#                                 NVL(reach, 0) AS engagements,
#                                 NVL(totalCurrency, 0) AS spend,
#                                 NVL(videoplay, 0) AS videoplay,
#                                 NVL(videoP25, 0) AS videoP25,
#                                 NVL(videoP50, 0) AS videoP50,
#                                 NVL(videoP75, 0) AS videoP75,
#                                 NVL(videoP100, 0) AS videoP100
#                             FROM {database}.gfa_series_stats
#                             WHERE Date >= '{date}'
#                             AND LOWER(campaignName) LIKE '%_re'
#                             )
#                         GROUP BY
#                             Date, Network_sub, OS_Name,
#                             campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
#                         ) a

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
#                         FROM {database}.ad_series_deliver_stats
#                         WHERE Date >= '{date}'
#                         AND Network_sub = 'NAVER_GFA'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) b
#                     ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.OS_Name = b.OS_Name AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.creativeName = b.CreativeName

#                     LEFT JOIN (
#                         SELECT Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName, 
#                             SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
#                             SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
#                         FROM {database}.ad_series_cohort_stats
#                         WHERE Date >= '{date}'
#                         AND cohort = 're'
#                         AND Network_sub = 'NAVER_GFA'
#                         GROUP BY Date, Network_sub, OS_Name, CampaignName, AdgroupName, creativeName) c
#                     ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.OS_Name = a.OS_Name AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.AdgroupName AND c.creativeName = a.CreativeName

#                     LEFT JOIN
#                         {database}.ad_series_map_creative_title d
#                     ON LOWER(a.creativeName) = LOWER(d.Creative)

#                     LEFT JOIN
#                         {database}.ad_series_map_media_network e
#                     ON a.Network_sub = e.`매체`
#         """            
#     df = spark.sql(insert_query)
#     log.info(f"[SUCCESS-INSERT-SERIES-NAVERGFA-REPORT]")
# except Exception as e:
#     print(e)
#     log.error(f"[FAIL-INSERT-SERIES-NAVERGFA-REPORT]{e}")
#     raise e

# COMMAND ----------

############################################################ [APPLE SEARCH ADS] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network AS Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.keywordId AS creativeId, a.keyword AS creativeName, NULL AS title, a.OS_Name,
                        a.impressions, a.clicks, NULL AS engagements, a.spend AS spend_usd, a.spend_kw AS spend, a.ExchangeRate_kw,
                        NULL AS videoplay, NULL AS videoP25, NULL AS videoP50, NULL AS videoP75, NULL AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            Date,
                            EXTRACT(week FROM Date) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM Date), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            Network,
                            'ios' AS OS_Name,
                            camp.campaignId,
                            campaignName,
                            adgroupId,
                            adgroupName,
                            keywordId,
                            keyword,
                            SUM(impressions) AS impressions,
                            SUM(clicks) AS clicks,
                            SUM(installs) AS installs,
                            SUM(spend) AS spend,
                            SUM(spend)*1.12*exc.ExchangeRate AS spend_kw,
                            exc.ExchangeRate AS ExchangeRate_kw
                        FROM (
                            SELECT 
                                Date, 'Apple Search Ads' AS Network, campaignId, adGroupId, adGroupName, 
                                CASE
                                    WHEN campaignId = '421779836' THEN '000000000'
                                    ELSE keywordId END AS keywordId,
                                CASE
                                    WHEN campaignId = '421779836' THEN 'unknown'
                                    ELSE keyword END AS keyword,
                                NVL(impressions, 0) AS impressions,
                                NVL(taps, 0) AS clicks,
                                NVL(installs, 0) AS installs,
                                NVL(localSpend_amount, 0) AS spend
                            FROM {database}.asa_series_adkeyword_stats
                            WHERE Date >= '{date}'
                            UNION ALL
                            SELECT 
                                Date, 'Apple Search Ads' AS Network, campaignId, adGroupId, adGroupName,
                                CASE
                                    WHEN campaignId = '421779836' THEN '000000000'
                                    ELSE keywordId END AS keywordId,
                                CASE
                                    WHEN campaignId = '421779836' THEN 'unknown'
                                    ELSE keyword END AS keyword,
                                NVL(impressions, 0) AS impressions,
                                NVL(taps, 0) AS clicks,
                                NVL(installs, 0) AS installs,
                                NVL(localSpend_amount, 0) AS spend
                            FROM {database}.asa_series_search_stats
                            WHERE adGroupName = 'WN_탐색_SearchMatch'
                            AND Date >= '{date}'
                            UNION ALL
                            SELECT 
                                Date, 'Apple Search Ads' AS Network, campaignId, adGroupId, adGroupName, 
                                CASE
                                    WHEN campaignId = '1205287032' THEN '000000001' END AS keywordId,
                                CASE
                                    WHEN campaignId = '1205287032' THEN 'unknown' END AS keyword,
                                NVL(impressions, 0) AS impressions,
                                NVL(taps, 0) AS clicks,
                                NVL(installs, 0) AS installs,
                                NVL(localSpend_amount, 0) AS spend
                            FROM {database}.asa_series_adgroup_stats
                            WHERE Date >= '{date}'
                            ) stat
                        LEFT JOIN
                            (SELECT campaignId, campaignName FROM hive_metastore.{database}.asa_series_camp) camp ON stat.campaignId = camp.campaignId
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.Date, 'yyyy-MM')
                            
                        GROUP BY
                            Date, Network, OS_Name,
                            camp.campaignId, campaignName, adgroupId, adgroupName, keywordId, keyword,
                            exc.ExchangeRate
                        ) a
                    
                        LEFT JOIN
                            {database}.ad_series_deliver_stats b
                        ON a.Date = b.Date AND a.Network = b.Network_sub AND a.CampaignId = b.CampaignId AND a.AdgroupId = b.AdgroupId AND a.keywordId = b.CreativeId
                            AND b.Date >= '{date}'
                            AND b.Network_sub = 'Apple Search Ads'

                        LEFT JOIN
                            {database}.ad_series_cohort_stats c
                        ON a.Date = c.Date AND a.Network = c.Network_sub AND a.CampaignId = c.CampaignId AND a.AdgroupId = c.AdgroupId AND a.keywordId = c.creativeId
                            AND c.Date >= '{date}'
                            AND c.cohort = 'ua'
                            AND c.Network_sub = 'Apple Search Ads'

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON a.Network = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-ASA-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-ASA-REPORT]{e}")
    raise e

# COMMAND ----------

############################################################ [Twitter] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            segmentDate AS Date,
                            EXTRACT(week FROM segmentDate) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM segmentDate), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            CASE
                                WHEN CampaignName LIKE '%_UA%' THEN 'Twitter Installs'
                                WHEN CampaignName LIKE '%_RE%' THEN 'Twitter RE'
                                ELSE 'Twitter_SNS' END AS Network_sub,
                            CASE
                                WHEN CampaignName LIKE '%_AOS%' THEN 'android'
                                WHEN CampaignName LIKE '%_IOS%' THEN 'ios'
                                ELSE 'unknown' END AS OS_Name,
                            CampaignId,
                            CampaignName,
                            lineItemId AS AdgroupId,
                            lineItemName AS adgroupName,
                            tweetId AS CreativeId,
                            name AS CreativeName,
                            SUM(NVL(metricsImpressions, 0)) AS impressions,
                            SUM(NVL(metricsAppClicks, 0)) AS clicks,
                            SUM(NVL(metricsEngagements, 0)) AS engagements,
                            SUM(NVL(metricsVideoTotalViews, 0)) AS videoplay,
                            SUM(NVL(metricsVideoViews25, 0)) AS videoP25,
                            SUM(NVL(metricsVideoViews50, 0)) AS videoP50,
                            SUM(NVL(metricsVideoViews75, 0)) AS videoP75,
                            SUM(NVL(metricsVideoViews100, 0)) AS videoP100,
                            CASE
                                WHEN LOWER(CampaignName) NOT LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0))*1.12/1000000
                                WHEN LOWER(CampaignName) LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0)*exc.ExchangeRate)*1.12/1000000 END AS spend
                        FROM {database}.twt_series_ad_stats stat
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.segmentDate, 'yyyy-MM')
                        WHERE segmentDate >= '{date}'
                        GROUP BY
                                segmentDate,
                                Network_sub, OS_Name,
                                CampaignId, CampaignName, AdgroupId, AdgroupName, CreativeId, CreativeName
                        HAVING Network_sub = 'Twitter Installs'
                        ) a
                        
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                        FROM {database}.ad_series_deliver_stats
                        WHERE Date >= '{date}'
                        AND Network_sub = 'Twitter Installs'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) b
                    ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.CampaignName = b.CampaignName AND a.CreativeId = b.AdgroupName AND a.AdgroupId = b.CreativeName
                    
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, 
                            SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                            SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                        FROM {database}.ad_series_cohort_stats
                        WHERE Date >= '{date}'
                        AND cohort = 'ua'
                        AND Network_sub = 'Twitter Installs'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) c
                    ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.CreativeId AND c.CreativeName = a.AdgroupId
                    
                    LEFT JOIN
					    {database}.ad_series_map_creative_title d
					ON LOWER(a.creativeName) = LOWER(d.Creative)
					
					LEFT JOIN
					    {database}.ad_series_map_media_network e
					ON a.Network_sub = e.`매체`

            UNION ALL

                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            segmentDate AS Date,
                            EXTRACT(week FROM segmentDate) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM segmentDate), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            CASE
                                WHEN CampaignName LIKE '%_UA%' THEN 'Twitter Installs'
                                WHEN CampaignName LIKE '%_RE%' THEN 'Twitter RE'
                                ELSE 'Twitter_SNS' END AS Network_sub,
                            CASE
                                WHEN CampaignName LIKE '%_AOS%' THEN 'android'
                                WHEN CampaignName LIKE '%_IOS%' THEN 'ios'
                                ELSE 'unknown' END AS OS_Name,
                            MAX(CampaignId) AS CampaignId,
                            CampaignName,
                            MAX(lineItemId) AS AdgroupId,
                            lineItemName AS adgroupName,
                            MAX(tweetId) AS CreativeId,
                            name AS CreativeName,
                            SUM(NVL(metricsImpressions, 0)) AS impressions,
                            SUM(NVL(metricsAppClicks, 0)) AS clicks,
                            SUM(NVL(metricsEngagements, 0)) AS engagements,
                            SUM(NVL(metricsVideoTotalViews, 0)) AS videoplay,
                            SUM(NVL(metricsVideoViews25, 0)) AS videoP25,
                            SUM(NVL(metricsVideoViews50, 0)) AS videoP50,
                            SUM(NVL(metricsVideoViews75, 0)) AS videoP75,
                            SUM(NVL(metricsVideoViews100, 0)) AS videoP100,
                            CASE
                                WHEN LOWER(CampaignName) NOT LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0))*1.12/1000000
                                WHEN LOWER(CampaignName) LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0)*exc.ExchangeRate)*1.12/1000000 END AS spend
                        FROM {database}.twt_series_ad_stats stat
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.segmentDate, 'yyyy-MM')
                        WHERE segmentDate >= '{date}'
                        GROUP BY
                                segmentDate,
                                Network_sub,
                                CampaignName, AdgroupName, CreativeName
                        HAVING Network_sub = 'Twitter_SNS'
                        ) a
                        
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                        FROM {database}.ad_series_deliver_stats
                        WHERE Date >= '{date}'
                        AND Network_sub = 'Twitter_SNS'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) b
                    ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.CampaignName = b.CampaignName AND a.AdgroupName = b.AdgroupName AND a.CreativeName = b.CreativeName
                    
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, 
                            SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                            SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                        FROM {database}.ad_series_cohort_stats
                        WHERE Date >= '{date}'
                        AND cohort = 'all'
                        AND Network_sub = 'Twitter_SNS'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) c
                    ON a.Date = c.Date AND a.Network_sub = c.Network_sub AND a.CampaignName = c.CampaignName AND a.AdgroupName = c.AdgroupName AND a.CreativeName = c.CreativeName
                    
                    LEFT JOIN
					    {database}.ad_series_map_creative_title d
					ON LOWER(a.creativeName) = LOWER(d.Creative)
					
					LEFT JOIN
					    {database}.ad_series_map_media_network e
					ON a.Network_sub = e.`매체`

            UNION ALL

                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, a.videoplay, a.videoP25, a.videoP50, a.videoP75, a.videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            segmentDate AS Date,
                            EXTRACT(week FROM segmentDate) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM segmentDate), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            CASE
                                WHEN CampaignName LIKE '%_UA%' THEN 'Twitter Installs'
                                WHEN CampaignName LIKE '%_RE%' THEN 'Twitter RE'
                                ELSE 'Twitter_SNS' END AS Network_sub,
                            CASE
                                WHEN CampaignName LIKE '%_AOS%' THEN 'android'
                                WHEN CampaignName LIKE '%_IOS%' THEN 'ios'
                                ELSE 'unknown' END AS OS_Name,
                            CampaignId,
                            CampaignName,
                            lineItemId AS AdgroupId,
                            lineItemName AS adgroupName,
                            tweetId AS CreativeId,
                            name AS CreativeName,
                            SUM(NVL(metricsImpressions, 0)) AS impressions,
                            SUM(NVL(metricsAppClicks, 0)) AS clicks,
                            SUM(NVL(metricsEngagements, 0)) AS engagements,
                            SUM(NVL(metricsVideoTotalViews, 0)) AS videoplay,
                            SUM(NVL(metricsVideoViews25, 0)) AS videoP25,
                            SUM(NVL(metricsVideoViews50, 0)) AS videoP50,
                            SUM(NVL(metricsVideoViews75, 0)) AS videoP75,
                            SUM(NVL(metricsVideoViews100, 0)) AS videoP100,
                            CASE
                                WHEN LOWER(CampaignName) NOT LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0))*1.12/1000000
                                WHEN LOWER(CampaignName) LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0)*exc.ExchangeRate)*1.12/1000000 END AS spend
                        FROM {database}.twt_series_ad_stats stat
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.segmentDate, 'yyyy-MM')
                        WHERE segmentDate >= '{date}'
                        GROUP BY
                                segmentDate,
                                Network_sub,
                                CampaignId, CampaignName, AdgroupId, AdgroupName, CreativeId, CreativeName
                        HAVING Network_sub = 'Twitter RE'
                        ) a
                        
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                        FROM {database}.ad_series_deliver_stats
                        WHERE Date >= '{date}'
                        AND Network_sub = 'Twitter RE'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) b
                    ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.CampaignName = b.CampaignName AND a.CreativeId = b.AdgroupName AND a.AdgroupId = b.CreativeName
                        
                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, 
                            SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                            SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                        FROM {database}.ad_series_cohort_stats
                        WHERE Date >= '{date}'
                        AND cohort = 're'
                        AND Network_sub = 'Twitter RE'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) c
                    ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.CreativeId AND c.CreativeName = a.AdgroupId
                    
                    LEFT JOIN
					    {database}.ad_series_map_creative_title d
					ON LOWER(a.creativeName) = LOWER(d.Creative)
					
					LEFT JOIN
					    {database}.ad_series_map_media_network e
					ON a.Network_sub = e.`매체`
                    
            UNION ALL
            
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        NULL AS campaignId, a.campaignName, NULL AS adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, 0 AS videoplay, 0 AS videoP25, 0 AS videoP50, 0 AS videoP75, 0 AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            segmentDate AS Date,
                            EXTRACT(week FROM segmentDate) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM segmentDate), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            CASE
                                WHEN CampaignName LIKE '%_UA%' THEN 'Twitter Installs'
                                WHEN CampaignName LIKE '%_RE%' THEN 'Twitter RE'
                                ELSE 'Twitter_SNS' END AS Network_sub,
                            CASE
                                WHEN CampaignName LIKE '%_AOS%' THEN 'android'
                                WHEN CampaignName LIKE '%_IOS%' THEN 'ios'
                                ELSE 'unknown' END AS OS_Name,
                            CampaignName,
                            lineItemName AS adgroupName,
                            tweetId AS CreativeId,
                            CreativeName,
                            SUM(NVL(metricsImpressions, 0)) AS impressions,
                            SUM(NVL(metricsLinkClicks, 0)) AS clicks,
                            SUM(NVL(metricsEngagements, 0)) AS engagements,
                            SUM(NVL(metricsVideoTotalView, 0)) AS videoplay,
                            SUM(NVL(metricsVideoViews25, 0)) AS videoP25,
                            SUM(NVL(metricsVideoViews50, 0)) AS videoP50,
                            SUM(NVL(metricsVideoViews75, 0)) AS videoP75,
                            SUM(NVL(metricsVideoViews100, 0)) AS videoP100,
                            CASE
                                WHEN LOWER(CampaignName) NOT LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0))*1.12/1000000
                                WHEN LOWER(CampaignName) LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0)*exc.ExchangeRate)*1.12/1000000 END AS spend
                        FROM {database}.twt_series_organic_stats stat
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.segmentDate, 'yyyy-MM')
                        WHERE segmentDate >= '{date}'
                        GROUP BY
                                segmentDate,
                                Network_sub, OS_Name,
                                CampaignName, AdgroupName, CreativeId, CreativeName
                        HAVING Network_sub = 'Twitter Installs'
                        ) a

                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                        FROM {database}.ad_series_deliver_stats
                        WHERE Date >= '{date}'
                        AND Network_sub = 'Twitter Installs'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) b
                    ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.CampaignName = b.CampaignName AND a.CreativeId = b.AdgroupName

                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, 
                            SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                            SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                        FROM {database}.ad_series_cohort_stats
                        WHERE Date >= '{date}'
                        AND cohort = 'ua'
                        AND Network_sub = 'Twitter Installs'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) c
                    ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.CreativeId

                    LEFT JOIN
                        {database}.ad_series_map_creative_title d
                    ON LOWER(a.creativeName) = LOWER(d.Creative)

                    LEFT JOIN
                        {database}.ad_series_map_media_network e
                    ON a.Network_sub = e.`매체`

            UNION ALL

                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network_sub, e.`네트워크명` AS campaignGoal,
                        NULL AS campaignId, a.campaignName, NULL AS adgroupId, a.adgroupName, a.creativeId, a.creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, a.engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, 0 AS videoplay, 0 AS videoP25, 0 AS videoP50, 0 AS videoP75, 0 AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            segmentDate AS Date,
                            EXTRACT(week FROM segmentDate) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM segmentDate), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            CASE
                                WHEN CampaignName LIKE '%_UA%' THEN 'Twitter Installs'
                                WHEN CampaignName LIKE '%_RE%' THEN 'Twitter RE'
                                ELSE 'Twitter_SNS' END AS Network_sub,
                            CASE
                                WHEN CampaignName LIKE '%_AOS%' THEN 'android'
                                WHEN CampaignName LIKE '%_IOS%' THEN 'ios'
                                ELSE 'unknown' END AS OS_Name,
                            CampaignName,
                            lineItemName AS adgroupName,
                            tweetId AS CreativeId,
                            CreativeName,
                            SUM(NVL(metricsImpressions, 0)) AS impressions,
                            SUM(NVL(metricsLinkClicks, 0)) AS clicks,
                            SUM(NVL(metricsEngagements, 0)) AS engagements,
                            SUM(NVL(metricsVideoTotalView, 0)) AS videoplay,
                            SUM(NVL(metricsVideoViews25, 0)) AS videoP25,
                            SUM(NVL(metricsVideoViews50, 0)) AS videoP50,
                            SUM(NVL(metricsVideoViews75, 0)) AS videoP75,
                            SUM(NVL(metricsVideoViews100, 0)) AS videoP100,
                            CASE
                                WHEN LOWER(CampaignName) NOT LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0))*1.12/1000000
                                WHEN LOWER(CampaignName) LIKE '%_va' THEN SUM(NVL(metricsBilledChargeLocalMicro, 0)*exc.ExchangeRate)*1.12/1000000 END AS spend
                        FROM {database}.twt_series_organic_stats stat
                        LEFT JOIN
                            (SELECT DATE_FORMAT(DATE_TRUNC('MONTH', DATE_ADD(Month, 31)), 'yyyy-MM') AS Month, ExchangeRate FROM {database}.ad_series_map_asa_exchangerate) exc
                        ON exc.Month = DATE_FORMAT(stat.segmentDate, 'yyyy-MM')
                        WHERE segmentDate >= '{date}'
                        GROUP BY
                                segmentDate,
                                Network_sub, OS_Name,
                                CampaignName, AdgroupName, CreativeId, CreativeName
                        HAVING Network_sub = 'Twitter_SNS'
                        ) a

                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                        FROM {database}.ad_series_deliver_stats
                        WHERE Date >= '{date}'
                        AND Network_sub = 'Twitter_SNS'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) b
                    ON a.Date = b.Date AND a.Network_sub = b.Network_sub AND a.CampaignName = b.CampaignName AND a.CreativeId = b.AdgroupName

                    LEFT JOIN (
                        SELECT Date, Network_sub, CampaignName, AdgroupName, creativeName, 
                            SUM(Uq_CV) AS Uq_CV, SUM(Uq_Use_ticket) AS Uq_Use_ticket, SUM(Uq_Use_Cookie_CU) AS Uq_Use_Cookie_CU, SUM(Use_ticket_CU) AS Use_ticket_CU, SUM(Use_ticket) AS Use_ticket,
                            SUM(Use_Cookie_CU) AS Use_Cookie_CU, SUM(Use_Cookie) AS Use_Cookie, SUM(CV_CU) AS CV_CU, SUM(CV) AS CV, SUM(retainedUsers) AS retainedUsers
                        FROM {database}.ad_series_cohort_stats
                        WHERE Date >= '{date}'
                        AND cohort = 'all'
                        AND Network_sub = 'Twitter_SNS'
                        GROUP BY Date, Network_sub, CampaignName, AdgroupName, creativeName) c
                    ON c.Date = a.Date AND c.Network_sub = a.Network_sub AND c.CampaignName = a.CampaignName AND c.AdgroupName = a.CreativeId

                    LEFT JOIN
                        {database}.ad_series_map_creative_title d
                    ON LOWER(a.creativeName) = LOWER(d.Creative)

                    LEFT JOIN
                        {database}.ad_series_map_media_network e
                    ON a.Network_sub = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-TWT-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-TWT-REPORT]{e}")
    raise e

# COMMAND ----------

############################################################ [TikTok] ############################################################
try:
    insert_query = f"""
            INSERT INTO {database}.series_report_d
                    SELECT DISTINCT
                        a.Date, a.week, a.dayoftheweek, a.Network AS Network_sub, e.`네트워크명` AS campaignGoal,
                        a.campaignId, a.campaignName, a.adgroupId, a.adgroupName, a.creativeId,
                        CASE WHEN a.CreativeName like '%.%' THEN LEFT(a.creativeName, CHARINDEX('.mp4', a.creativeName)-1) ELSE a.CreativeName END AS creativeName, d.title, a.OS_Name,
                        a.impressions, a.clicks, NULL AS engagements, NULL AS spend_usd, a.spend, NULL AS ExchangeRate_kw, NULL AS videoplay, NULL AS videoP25, NULL AS videoP50, NULL AS videoP75, NULL AS videoP100,
                        b.installs, b.reattributions,
                        c.Uq_CV, c.Uq_Use_ticket, c.Uq_Use_Cookie_CU, c.Use_ticket_CU, c.Use_ticket, c.Use_Cookie_CU, c.Use_Cookie, c.CV_CU, c.CV,
                        b.dailyActiveUsers, c.retainedUsers, NULL AS installSKAN
                    FROM (
                        SELECT
                            stat_time_day AS Date,
                            EXTRACT(week FROM stat_time_day) AS week,
                            DECODE(EXTRACT(DAYOFWEEK FROM stat_time_day), 2,'mon',3,'tue', 4,'wed', 5,'thu', 6,'fri', 7,'sat',1,'sun') AS dayoftheweek,
                            'TikTok' AS Network,
                            LOWER(camp.operating_systems) AS OS_Name,
                            camp.campaign_id AS campaignId,
                            camp.campaign_name AS campaignName,
                            camp.adgroup_id AS adgroupId,
                            camp.adgroup_name AS adgroupName,
                            stat.ad_id AS creativeId,
                            stat.ad_name AS creativeName,
                            SUM(NVL(impressions, 0)) AS impressions,
                            SUM(NVL(clicks, 0)) AS clicks,
                            SUM(NVL(app_install, 0)) AS installs,
                            SUM(NVL(spend, 0)) AS spend
                        FROM {database}.tik_series_adkeyword_stats stat
                        LEFT JOIN (
                             SELECT DISTINCT temp.campaign_id, temp.campaign_name, temp.adgroup_id, temp.adgroup_name, temp.ad_id, temp.ad_name, os.operating_systems
                             FROM {database}.tik_series_adkeyword temp
                             LEFT JOIN (
                                 SELECT DISTINCT adgroup_id, campaign_id, operating_systems FROM {database}.tik_series_adgroup) os
                             ON temp.campaign_id = os.campaign_id AND temp.adgroup_id = os.adgroup_id) camp ON stat.ad_id = camp.ad_id
                            WHERE stat.stat_time_day >= '{date}'
                        GROUP BY
                            Date, Network, OS_Name,
                            campaignId, campaignName, adgroupId, adgroupName, creativeId, creativeName
                    ) a

                        LEFT JOIN (
                            SELECT Date, Network_sub, CampaignId, CampaignName, AdgroupId, AdgroupName, creativeId, creativeName, SUM(installs) AS installs, SUM(reattributions) AS reattributions, SUM(dailyActiveUsers) AS dailyActiveUsers
                            FROM {database}.ad_series_deliver_stats
                            WHERE Date >= '{date}'
                            AND Network_sub = 'TikTok'
                            GROUP BY Date, Network_sub, CampaignId, CampaignName, AdgroupId, AdgroupName, creativeId, creativeName) b
                        ON a.Date = b.Date AND a.Network = b.Network_sub AND a.CampaignId = b.CampaignId AND a.AdgroupId = b.AdgroupId AND a.creativeId = b.CreativeId

                        LEFT JOIN
                            {database}.ad_series_cohort_stats c
                        ON a.Date = c.Date AND a.Network = c.Network_sub AND a.OS_Name = c.OS_Name AND a.CampaignId = c.CampaignId AND a.AdgroupId = c.AdgroupId AND a.creativeId = c.creativeId
                            AND c.Date >= '{date}'
                            AND c.cohort = 'ua'
                            AND c.Network_sub = 'TikTok'
                        
                        LEFT JOIN
                            {database}.ad_series_map_creative_title d
                        ON LOWER(a.creativeName) = LOWER(d.Creative)

                        LEFT JOIN
                            {database}.ad_series_map_media_network e
                        ON a.Network = e.`매체`
        """            
    df = spark.sql(insert_query)
    log.info(f"[SUCCESS-INSERT-SERIES-TIKTOK-REPORT]")
except Exception as e:
    print(e)
    log.error(f"[FAIL-INSERT-SERIES-TIKTOK-REPORT]{e}")
    raise e

# COMMAND ----------

try:
    insert_query = f"""
update auto_report.series_report_d
set creativeName = 'Sihanbu_Carousel_Novel_2'
where date in ('2023-01-25', '2023-01-26')
and Network_sub in ('Twitter RE', 'Twitter Installs')
and creativeId in ('1616345274158559235', '1616344359687032832')
        """            
    df = spark.sql(insert_query)
except Exception as e:
    print(e)

# COMMAND ----------

try:
    insert_query = f"""
update auto_report.series_report_d
set creativeName = 'Sihanbu_Carousel_Webtoon_5_B'
where date in ('2023-01-25', '2023-01-26')
and Network_sub in ('Twitter RE', 'Twitter Installs')
and creativeId in ('1616344927507738627', '1616344746074738688')
        """            
    df = spark.sql(insert_query)
except Exception as e:
    print(e)

# COMMAND ----------

