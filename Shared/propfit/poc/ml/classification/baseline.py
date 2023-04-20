# Databricks notebook source
# MAGIC %md
# MAGIC # 0. Import Library

# COMMAND ----------

from datetime import date, timedelta, datetime

import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
# %matplotlib inline

import pyspark.sql.functions as F
import pyspark.sql.types as T

import re

from pytz import timezone
KST = timezone('Asia/Seoul')

pd.options.display.max_rows = 100

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Import Data
# MAGIC * tid를 기준으로 데이터를 정리할 예정으로 DeviceId 간 중복이 허용됨

# COMMAND ----------

# MAGIC %md
# MAGIC ## a. propfit
# MAGIC * **ad_adv /** 1: 웰릭스렌탈, 2: 피플카, 3: 공단기, 4: 쓱고우, 5: 미건라이프, 6: 미니스탁 중 미건라이프 데이터만 사용
# MAGIC * **size /** 1: 320\*480, 2: 320\*50
# MAGIC * **deviceType /** 1: Mobile/Tablet, 2: PC, 3: Connected TV, 4: Phone, 5: Tablet, 6: Connected Device, 7:Set Top Box
# MAGIC * **appCategory /** 30가지 Category와 etc. 1가지로 총 31가지 카테고리로 구현
# MAGIC * **os /** 1:android, 2:ios, 3: android/ios/unknown이 아닌 운영체제, 4: unknown, missing: Null(해당 값은 향후 결측값 처리 방법 논의할 때 다시 검토)
# MAGIC * **region /** 대한민국 행정구역 ISO 3166-2 코드에 따라 1특별시, 6광역시, 1특별자치시, 8도, 1특별자치도 + "전체" + "대한민국이외"로 총 19가지 카테고리로 구현

# COMMAND ----------

import_propfit =  """
SELECT c.*, d.*
FROM cream.propfit_daily_raw c
LEFT JOIN (
        SELECT
                tid,
-- 디바이스 정보
                BidStream.DeviceId,
                LOWER(BidStream.request.device.os) AS os,
                LOWER(BidStream.request.device.make) AS deviceMake,
                BidStream.request.device.devicetype AS deviceType,
                CASE
                        WHEN BidStream.request.device.devicetype = '1' THEN 'Mobile/Tablet'
                        WHEN BidStream.request.device.devicetype = '2' THEN 'PC'
                        WHEN BidStream.request.device.devicetype = '3' THEN 'Connected TV'
                        WHEN BidStream.request.device.devicetype = '4' THEN 'Phone'
                        WHEN BidStream.request.device.devicetype = '5' THEN 'Tablet'
                        WHEN BidStream.request.device.devicetype = '6' THEN 'Connected Device'
                        WHEN BidStream.request.device.devicetype = '7' THEN 'Set Top Box'
                        ELSE BidStream.request.device.devicetype END AS deviceTypeD,
                LOWER(BidStream.request.device.model) AS deviceModel,
-- 지면 정보
                BidStream.request.imp.tagid[0] AS tagid,
                BidStream.bundle_domain AS url,
                CASE
                    WHEN REPLACE(b.category01, ' ', '') IN ('Books&Reference', '도서/참고자료', '도서') THEN '1'                                                   --'도서'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Business', '비즈니스') THEN '2'                                                                      --'비즈니스'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Tools', '도구', 'Libraries&Demo', '라이브러리/데모', '개발자도구') THEN '3'                            --'개발자 도구'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Education', '학습', '교육') THEN '4'                                                                 --'교육'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Entertainment', '엔터테인먼트') THEN '5'                                                              --'엔터테인먼트'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Finance', '금융') THEN '6'                                                                           --'금융'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Food&Drink', '식음료', '음식및음료') THEN '7'                                                         --'음식 및 음료'
                    WHEN REPLACE(b.category01, ' ', '') IN ('Action', '액션', 'Adventure', '어드벤처', 'Arcade', '아케이드', 'Board', '보드', 'Bubbleshooter',
                                                            'Card', '카드', 'Casino', '카지노', 'Casual', '캐주얼게임', '캐주얼', 'Educational', 'Multiplayer',
                                                            'Music', 'Puzzle', '퍼즐', 'Racing', '자동차경주', '레이싱', 'RolePlaying', '롤플레잉', 'Simulation',
                                                            '시뮬레이션', 'Singleplayer', '싱글플레이어', 'Sports', 'Strategy', '전략', 'Stylized', 'Trivia',
                                                            'Tycoon', 'Word', '단어', '퀴즈', '게임') THEN '8'                                                     --'게임'
                    WHEN b.category01 IN ('Art & Design', '예술/디자인', '그래픽 및 디자인') THEN '8'                                                         --'그래픽 및 디자인'
                    WHEN b.category01 IN ('Health & Fitness', '건강/운동', '건강 및 피트니스') THEN '9'                                                       --'건강 및 피트니스'
                    WHEN b.category01 IN ('Lifestyle', '라이프 스타일', '라이프스타일') THEN '10'                                                             --'라이프 스타일'
                    WHEN b.category01 IN ('News & Magazines', '뉴스', '뉴스/잡지', '뉴스 및 잡지') THEN '11'                                                  --'뉴스 및 잡지'
                    WHEN b.category01 IN ('Medical', '의료') THEN '12'                                                                                      --'의료'
                    WHEN b.category01 IN ('Music & Audio', '음악/오디오', '음악') THEN '13'                                                                  --'음악'
                    WHEN b.category01 IN ('Navigation', 'Maps & Navigation', '지도/내비게이션', '내비게이션') THEN '14'                                       --'내비게이션'
                    WHEN b.category01 IN ('Photography', '사진', 'Video Players & Editors', '동영상 플레이어/편집기', '사진 및 비디오') THEN '15'              --'사진 및 비디오'
                    WHEN b.category01 IN ('Productivity', '참고', '생산성') THEN '16'                                                                       --'생산성'
                    WHEN b.category01 IN ('Shopping', '쇼핑') THEN '17'                                                                                     --'쇼핑'
                    WHEN b.category01 IN ('Social', '소셜', '스티커', '소셜 네트워킹') THEN '18'                                                              --'소셜 네트워킹'
                    WHEN b.category01 IN ('Sports', '스포츠') THEN '19'                                                                                     --'스포츠'
                    WHEN b.category01 IN ('Travel & Local', '여행 및 지역정보', '여행') THEN '20'                                                            --'여행'
                    WHEN b.category01 IN ('Personalization', '맞춤 설정', '유틸리티') THEN '21'                                                              --'유틸리티'
                    WHEN b.category01 IN ('Weather', '날씨') THEN '22'                                                                                      --'날씨'
                    WHEN b.category01 IN ('Auto & Vehicles', '자동차', '자동차 및 차량') THEN '23'                                                           --'자동차 및 차량'
                    WHEN b.category01 IN ('Beauty', '뷰티') THEN '24'                                                                                       --'뷰티'
                    WHEN b.category01 IN ('Comics', '만화') THEN '25'                                                                                       --'만화'
                    WHEN b.category01 IN ('Communications', 'Communication', '커뮤니케이션') THEN '26'                                                       --'커뮤니케이션'
                    WHEN b.category01 IN ('Dating', '데이트') THEN '27'                                                                                     --'데이트'
                    WHEN b.category01 IN ('Events', '이벤트', '예약 및 예매') THEN '28'                                                                      --'예약 및 예매'
                    WHEN b.category01 IN ('House & Home', '부동산/홈 인테리어', '부동산 및 인테리어') THEN '29'                                                --'부동산 및 인테리어'
                    WHEN b.category01 IN ('Parenting', '출산/육아', '육아') THEN '30'                                                                        --'육아'
                    ELSE REPLACE(b.category01, ' ', '') END AS appCategory,
                FORMAT_NUMBER(BidStream.bidfloor, 12) AS bidfloor,
-- 유저 위치 정보
                BidStream.request.device.geo.country,
                CASE
                    WHEN LOWER(BidStream.request.device.geo.region) = 'kr' THEN 'KR-0'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('11', "ch'angsa-dong", 'dobong-gu', 'dongdaemun-gu', 'dongjak-gu',
                            'eunpyeong-gu', 'gangbuk-gu', 'gangdong-gu', 'gangnam-gu', 'gangseo-gu', 'geumcheon-gu', 'guro-gu', 'gwanak-gu',
                            'gwangjin-gu', 'hapjeong-dong', "ich'on-dong", 'jongno-gu', 'junggu', 'jung-gu', 'jungnang-gu',
                            'kr-11', 'mapo-gu', 'noryangjin-dong', 'nowon-gu', 'seocho', 'seocho-gu', 'seodaemun-gu', 'seongbuk-gu',
                            'seongdong-gu', 'seoul', "seoul_t'ukpyolsi", 'seoul-teukbyeolsi', 'sinsa-dong', 'soeul', 'songpa-gu', 'yangcheon-gu',
                            "yangch'on-gu", 'yeongdeungpo-gu', 'yongsan-gu') THEN 'KR-11'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('26', 'buk-gu', 'busan', 'busan-gwangyeoksi', 'busanjin-gu', 'donggu',
                            'dong-gu', 'geumjeong-gu', 'gijang', 'gijang-gun', 'haeundae', 'haeundae-gu', 'kr-26', 'nam-gu', 'saha-gu',
                            'sasang-gu', 'seo-gu', 'yeongdo-gu', 'yeonje-gu') THEN 'KR-26'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('27', 'daegu', 'daegu-gwangyeoksi', 'dalseo-gu', 'dalseong-gun', 'kr-27',
                            'suseong-gu', 'suyeong-gu') THEN 'KR-27'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('28', 'bupyeong', 'bupyeong-gu', 'galsan', 'ganghwa-gun', 'gyeyang-gu',
                            'incheon', 'incheon-gwangyeoksi', 'inhyeondong', 'juan-dong', 'kr-28', 'namdong-gu', 'ongjin-gun', 'unseodong',
                            'yeonsu-gu') THEN 'KR-28'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('29', 'gwangju', 'gwangju-gwangyeoksi', 'gwangsan-gu', 'kr-29') THEN 'KR-29'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('30', 'daedeok-gu', 'daejeon', 'daejeon-gwangyeoksi', 'kr-30', 'yuseong',
                            'yuseong-gu') THEN 'KR-30'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('31', 'kr-31', 'ulju-gun', 'ulsan', 'ulsan-gwangyeoksi') THEN 'KR-31'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('41', 'ansan-si', 'anseong', 'anyang-si', 'areannamkwaengi', 'bucheon-si',
                            'bundang-gu', 'dongan', 'dongducheon-si', 'gapyeong county', 'gimpo-si', 'goyang-si', 'gunpo', 'guri-si', 'gwacheon',
                            'gwacheon-si', 'gwangmyeong', 'gwangmyeong-si', 'gyeonggi', 'gyeonggi-do', 'hanam', 'hwaseong-si', 'icheon-si',
                            'jinjeop-eup', 'kosaek-tong', 'kr-41', 'masan-dong', 'namyangju', 'osan', 'paju', 'paju-si', 'pocheon', 'pocheon-si',
                            'pyeongtaek-si', 'seongnam-si', 'siheung-si', 'suji-gu', 'suwon', 'tokyang-gu', 'uijeongbu-si',
                            'uiwang', 'uiwang-si', 'yangju', "yangp'yong", 'yeoju', 'yeoncheon', 'yeoncheon-gun', 'yongin', 'yongin-si') THEN 'KR-41'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('42', 'cheorwon', "chinch'on", 'chuncheon', 'donghae-si', 'gangneung',
                            'gangwon', 'gangwon-do', 'goseong-gun', 'hoengseong-gun', 'hongcheon-gun', 'hwacheon', 'inje-gun', 'jeongseon-gun',
                            'kr-42', 'pyeongchang', 'samcheok', 'samcheok-si', 'sokcho', 'taebaek-si', 'tonghae', 'wonju', 'yanggu', 'yangyang',
                            'yangyang-gun', 'yeongwol-gun') THEN 'KR-42'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('43', 'boeun-gun', 'cheongju-si', 'cheongwon-gun', 'cheorwon-gun', 'chungbuk',
                            'chungcheongbuk-do', 'chungju', 'danyang', 'danyang-gun', 'eumseong', 'eumseong-gun', 'heungdeok-gu', 'jecheon',
                            'koesan', 'kr-43', 'okcheon', 'okcheon-gun', 'yeongdong', 'yeongdong-gun', 'yongam') THEN 'KR-43'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('44', 'asan', 'boryeong', 'boryeong-si', 'buyeo-gun', 'cheonan', 'cheongyang-gun',
                            'chungcheongnam-do', 'chungnam', 'geumsan', 'geumsan-gun', 'gongju', 'gyeryong', 'gyeryong-si', "hongch'on", 'hongseong',
                            'hongseong-gun', 'kr-44', 'nonsan', 'north chungcheong', 'seocheon-gun', 'seosan city', 'taean-gun', 'taian', 'ungcheon-eup',
                            'yesan') THEN 'KR-44'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('45', 'buan-gun', 'changsu', 'gimje-si', 'gochang', 'gunsan', 'iksan', 'imsil',
                            'jeollabuk-do', 'jeonbuk', 'jeongeup', 'jeonju', 'jinan-gun', 'kimje', "koch'ang", 'kr-45', 'muju-gun', 'samnye',
                            'sunchang-gun', 'wanju') THEN 'KR-45'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('46', 'boseong', 'boseong-gun', 'daeguri', 'damyang', 'damyang-gun', 'gangjin-gun',
                            'goheung-gun', 'gokseong-gun', 'gurye', 'gurye-gun', 'gwangyang', 'gwangyang-si', 'haenam', 'haenam-gun',
                            'hampyeong-gun', 'hwasun-gun', 'jangheung', 'jangseong', 'jeollanam-do', 'jeonnam', 'jindo-gun', 'kr-46', 'kurye', 'mokpo',
                            'mokpo-si', 'muan', 'naju', 'suncheon', 'suncheon-si', 'wando-gun', 'yeongam', 'yeongam-gun', 'yeonggwang', 'yeonggwang-gun',
                            'yeosu') THEN 'KR-46'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('47', 'andong', 'bonghwa-gun', 'cheongdo-gun', 'cheongsong', 'cheongsong gun',
                            'chilgok', 'chilgok-gun', 'gimcheon', 'goryeong-gun', 'gumi', 'gunwi-gun', 'gyeongbuk', 'gyeongju', 'gyeongsangbuk-do',
                            'gyeongsan-si', 'kr-47', 'mungyeong', 'pohang', 'pohang-si', 'sangju', 'seongju-gun', 'uiseong-gun', 'ulchin', 'uljin county',
                            'yecheon-gun', 'yeongcheon-si', 'yeongdeok', 'yeongdeok-gun', 'yeongju', 'yeongyang-gun') THEN 'KR-47'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('48', 'changnyeong', 'changwon', 'geochang-gun', 'geoje', 'geoje-si', 'gimhae',
                            'gyeongnam', 'gyeongsangnam-do', 'hadong', 'haman', 'haman-gun', 'hamyang-gun', 'hapcheon-gun', 'jinhae-gu',
                            'jinju', 'kr-48', 'miryang', 'namhae', 'namhae-gun', 'sacheon-si', 'sancheong-gun', 'tongyeong-si', 'uiryeong-gun', 'yangsan') THEN 'KR-48'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('49', 'jeju', 'jeju city', 'jeju-do', 'jeju-teukbyeoljachido', 'kr-49', 'seogwipo',
                            'seogwipo-si') THEN 'KR-49'
                    WHEN LOWER(BidStream.request.device.geo.region) IN ('50', 'kr-50', 'sejong', 'sejong-si') THEN 'KR-50'
                    ELSE NULL END AS region,
                FORMAT_STRING(BidStream.request.device.geo.lat) AS lat,
                FORMAT_STRING(BidStream.request.device.geo.lon) AS lon
        FROM ice.streams_bid_bronze_app_nhn a
        LEFT JOIN (SELECT requestAppBundle, category01 from ice.app_info_nhn) b ON a.BidStream.bundle_domain = b.requestAppBundle
        WHERE a.actiontime_date BETWEEN '2023-03-03' AND '2023-03-31'
) d ON c.tid = d.tid AND c.Date BETWEEN '2023-03-03' AND '2023-03-31'
WHERE ad_adv = '5'
"""
df = spark.sql(import_propfit)

# COMMAND ----------

df.display()

# COMMAND ----------

# (!) 2023-03-03 - 2023-03-13 :: 34834135
# df.filter("imptime is null").count()

# COMMAND ----------

## Categorical Feature 확인
# df.select(F.split('deviceModel', '-').getItem(0).alias('model')).drop_duplicates().show()
# df.select(F.col('region')).drop_duplicates().show() #.sort(F.col('coutry').desc())

# COMMAND ----------

## 프로핏 데이터 최종 확정
df_prop = df.withColumn('bid', F.when(df.bidtime.isNotNull(), '1').otherwise(F.lit(None)))\
            .withColumn('imp', F.when((df.bidtime.isNotNull()) & (df.imptime.isNotNull()), '1').otherwise('0'))\
            .withColumn('clk', F.when((df.bidtime.isNotNull()) & (df.imptime.isNotNull()) & (df.clktime.isNotNull()), '1').otherwise('0'))\
            .withColumn('size', F.when((df.Width == '320') & (df.Height == '480'), '1').when((df.Width == '320') & (df.Height == '50'), '2').otherwise(F.lit(None)))\
            .withColumn('os', F.when(df.os == 'android', '1').when(df.os == 'ios', '2').when(df.os == 'unknown', '4').when(df.os .isNull(), F.lit(None)).otherwise('3'))\
            .withColumn('appCategory', F.when(df.appCategory == '', F.lit(None)).otherwise(df.appCategory))\
            .withColumn('region', F.regexp_replace(df.region, 'KR-', ''))\
            .select(
                F.col('Date'), 
                F.col('c.DeviceId').alias('DeviceId'), 
                F.col('c.tid').alias('tid'),
                F.col('bid'), 
                F.col('imp'), 
                F.col('clk'),
                F.col('size'), 
                F.col('bidfloor'), 
                F.col('appCategory'),
                F.col('deviceType'), 
                F.col('os'), 
                F.col('region')) 
                    # F.col('ad_adv'), 
# display(df_prop)

# COMMAND ----------

# (!) if imp exist
# df_prop.filter("imp = '0'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. DMP: TG360

# COMMAND ----------

import_tg360 =  """SELECT * FROM ice.propfit_tg"""
df_tg_t = spark.sql(import_tg360)

import_mapkey = """SELECT * FROM ice.propfit_tg_code"""
df_map = spark.sql(import_mapkey)

# 1	gender_code
# 2	uti_category
# 3	product_category
# 4	app_category
# 5	carrier
# 6	online_category
# 7	age

# COMMAND ----------

## 행동데이터 식별코드 맵핑: 행동데이터 사용안하면 굳이 하지 않아도 됨!!
# df_app_t = df_tg_t.select(F.col('uuid'), F.explode_outer(df_tg_t.app_category))
# df_map_app = df_map.filter(F.col('category_code') == 'app_category').select(F.col('attribution_code'), F.col('attribution'))
# df_app = df_app_t.join(df_map_app, df_app_t.col == df_map_app.attribution_code, 'left')
# df_app = df_app.select(F.col('uuid').alias('id'), F.col('attribution')).groupby('id').agg(F.collect_list("attribution").alias('app_category_ko'))

# df_online_t = df_tg_t.select(F.col('uuid'), F.explode_outer(df_tg_t.online_category))
# df_map_online = df_map.filter(F.col('category_code') == 'online_category').select(F.col('attribution_code'), F.col('attribution'))
# df_online = df_online_t.join(df_map_online, df_online_t.col == df_map_online.attribution_code, 'left')
# df_online = df_online.select(F.col('uuid').alias('id'), F.col('attribution')).groupby('id').agg(F.collect_list("attribution").alias('online_category_ko'))

# df_prod_t = df_tg_t.select(F.col('uuid'), F.explode_outer(df_tg_t.product_category))
# df_map_prod = df_map.filter(F.col('category_code') == 'product_category').select(F.col('attribution_code'), F.col('attribution'))
# df_prod = df_prod_t.join(df_map_prod, df_prod_t.col == df_map_prod.attribution_code, 'left')
# df_prod = df_prod.select(F.col('uuid').alias('id'), F.col('attribution')).groupby('id').agg(F.collect_list("attribution").alias('product_category_ko'))

# df_uti_t = df_tg_t.select(F.col('uuid'), F.explode_outer(df_tg_t.uti_category))
# df_map_uti = df_map.filter(F.col('category_code') == 'uti_category').select(F.col('attribution_code'), F.col('attribution'))
# df_uti = df_uti_t.join(df_map_uti, df_uti_t.col == df_map_uti.attribution_code, 'left')
# df_uti = df_uti.select(F.col('uuid').alias('id'), F.col('attribution')).groupby('id').agg(F.collect_list("attribution").alias('uti_category_ko'))

# df_tg = df_tg_t.join(df_app, df_tg_t.uuid == df_app.id, 'left').drop('id')
# df_tg = df_tg.join(df_online, df_tg.uuid == df_online.id, 'left').drop('id')
# df_tg = df_tg.join(df_prod, df_tg.uuid == df_prod.id, 'left').drop('id')
# df_tg = df_tg.join(df_uti, df_tg.uuid == df_uti.id, 'left').drop('id')

#-------------------------------------------------------------------------------------------------------------------------------------#
## TG360 데이터 우선적으로 사용: 데모 데이터만 사용
df_tg = df_tg_t.filter(F.col('id_type') == 'ADID').select(F.col('uuid'), F.col('gender_code'), F.col('age_range'), F.col('carrier'))

# df_tg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## c. GA4: 미건라이프
# MAGIC - 3월 중 총 66건 발생

# COMMAND ----------

## 미건GA 데이터
import_ga =  """
        SELECT 
            Date, DeviceId, tid, user_pseudo_id,
            Revenue, ITCT,
            CASE WHEN newUser > 0 THEN '1' ELSE newUser END AS newUser,
            session_duration, pageview_event, s0_event, s25_event, s50_event, s75_event, s100_event,
            clkRequestMiso_event, clkCSR_event, requestLower_event AS clkRequestLower_event
        FROM cream.propfit_ga_migun_daily
        WHERE Date between '2023-03-03' and '2023-03-31'
"""

df_mg = spark.sql(import_ga).filter("Date is not null")



# (!) e.g. 상담 신청 등
df_mg = df_mg.withColumn("cv", F.when((F.col('clkRequestMiso_event') > 0) | (F.col('clkCSR_event') > 0) | (F.col('clkRequestLower_event') > 0), 1.0).otherwise(0.0))


# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Join Table
# MAGIC * 프로핏: 3/3~31일
# MAGIC * GA: 3/3~31일

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Join Propfit - DMP(TG360)

# COMMAND ----------

from pyspark.sql.types import *

df_join = df_prop.alias('a').join(df_tg, df_prop.DeviceId == df_tg.uuid, 'left')

df_join = df_join.select(
    F.col('a.*'),
    F.col('gender_code').alias('gender'), 
    F.col('age_range').alias('age'), 
    F.col('carrier')
).withColumn("gender", F.col('gender').cast(StringType()))

# COMMAND ----------

df_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Join Propfit - DMP(TG360) - GA4

# COMMAND ----------

df_join = df_join.alias('a').join(df_mg.alias('b'), (df_join.tid == df_mg.tid) & (df_join.DeviceId == df_mg.DeviceId), 'left')

## 요일:1=sun, 2=mon, 3=tue, 4=wed, 5=thu, 6=fri, 7=sat
df_join = df_join.select(
    F.col('a.Date'), 
    F.dayofweek('a.Date').alias('dow'),
    F.col('a.DeviceId'), 
    F.col('a.tid'), 
    F.col('user_pseudo_id'),
    F.col('bid'), 
    F.col('imp'), 
    F.col('clk'),
#                          F.col('ad_adv'),
    F.col('gender'), 
    F.col('age'), 
    F.col('carrier'),
    F.col('size'), 
    F.col('bidfloor'), 
    F.col('appCategory'),
    F.col('deviceType'), 
    F.col('os'), 
    F.col('region'),
    F.col('Revenue'), 
    F.col('ITCT'), 
    F.col('newUser'), 
    F.col('session_duration'),
    F.col('pageview_event'), 
    F.col('s0_event'), 
    F.col('s25_event'), 
    F.col('s50_event'), 
    F.col('s75_event'), 
    F.col('s100_event'),
    F.col('cv'), 
    F.col('clkRequestMiso_event'), 
    F.col('clkCSR_event'), 
    F.col('clkRequestLower_event')
).withColumn("dow", F.col('dow').cast(StringType()))\
.withColumn("bidfloor", F.col('bidfloor').cast(DoubleType()))\
.withColumn("bid", F.col('bid').cast(StringType()))\
.withColumn("imp", F.col('imp').cast(StringType()))\
.withColumn("clk", F.col('clk').cast(StringType()))\
.withColumn("newUser", F.col('newUser').cast(StringType()))\
.withColumn("deviceType", F.col('deviceType').cast(StringType()))
#                  .withColumn("ad_adv", F.col('ad_adv').cast(StringType()))\

# print(f'df_mg 행 수-temp: ' + str(df_mg.count()))
# print(f'df_prop 행 수: ' + str(df_prop.select(F.countDistinct('tid')).collect()[0][0]))
# print(f'df_prop ADID 수: ' + str(df_prop.select(F.countDistinct('DeviceId')).collect()[0][0]))
# print(f'df_mg 행 수: ' + str(df_mg.select(F.countDistinct('tid')).collect()[0][0]))
# print(f'df_mg ADID 수: ' + str(df_mg.select(F.countDistinct('DeviceId')).collect()[0][0]))
# print(f'df_join 행 수-temp: ' + str(df_join.count()))

# print(f'df_join 행 수: ' + str(df_join.select(F.countDistinct('a.tid')).collect()[0][0]))
# print(f'df_join ADID 수: ' + str(df_join.select(F.countDistinct('a.DeviceId')).collect()[0][0]))
#--#--#--#--#--#--#--#--#--#--#--#--#
# # df_mg 행 수-temp: 34008
# # df_prop 행 수: 119190949
# # df_prop ADID 수: 7806948
# # df_mg 행 수: 30923
# # df_mg ADID 수: 19848
# # df_join 행 수-temp: 119,194,034
# # df_join 행 수: 119190949
# # df_join ADID 수: 7806948
#--#--#--#--#--#--#--#--#--#--#--#--#
# display(df_join)

# COMMAND ----------

df_join.count()

# COMMAND ----------



# COMMAND ----------

# sns.pairplot(df_join.toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ## a. 데이터 타입 별 확인사항
# MAGIC * 특히 StringType 변수의 경우 빈 string('')있는지 확인하기!
# MAGIC * 모델에 활용할 데이터 최종 검토하기
# MAGIC   * 'Date', 'bid' 데이터 제외

# COMMAND ----------

# display(df_join)

# COMMAND ----------

df_join_cv = df_join.select(F.col('tid'), F.col('cv'))
df_join = df_join.drop("Date", "bid", "cv")

# COMMAND ----------

from collections import defaultdict

data_types = defaultdict(list)
for entry in df_join.schema.fields:
    data_types[str(entry.dataType)].append(entry.name)
    
data_types

# 'DateType', 'StringType', 'DoubleType', 'LongType' 으로 구성

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. 범주형 데이터 처리

# COMMAND ----------

## 범주형 데이터 분리

from pyspark.sql.functions import countDistinct, approxCountDistinct

counts_summary = df_join.agg(*[countDistinct(c).alias(c) for c in data_types["StringType"]])
counts_summary = counts_summary.toPandas()

counts = pd.Series(counts_summary.values.ravel())
counts.index = counts_summary.columns

sorted_vars = counts.sort_values(ascending = False)
ignore = list((sorted_vars[sorted_vars > 100]).index)

strings_used = [var for var in data_types["StringType"] if var not in ignore]

# COMMAND ----------

strings_used

# COMMAND ----------

## 결측값 처리 : 일괄적으로 "missing"으로 우선적 처리

missing_data_fill = {}
for var in strings_used:
    missing_data_fill[var] = "missing"

df = df_join.fillna(missing_data_fill)

# COMMAND ----------

## OneHotEncoding

from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer

stage_string = [StringIndexer(inputCol= c, outputCol= c+"_string_encoded") for c in strings_used]
stage_one_hot = [OneHotEncoder(inputCol= c+"_string_encoded", outputCol= c+ "_one_hot") for c in strings_used]

ppl = Pipeline(stages= stage_string + stage_one_hot)
df = ppl.fit(df).transform(df)

# COMMAND ----------

stage_string

# COMMAND ----------

stage_one_hot

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## c. 수치형 데이터 처리
# MAGIC * DoubleType
# MAGIC * LongType

# COMMAND ----------

numericals = data_types["DoubleType"]
numericals = [var for var in numericals if var not in ignore]
numericals_imputed = [var + "_imputed" for var in numericals]

from pyspark.ml.feature import Imputer

imputer = Imputer(inputCols = numericals, outputCols = numericals_imputed)
df = imputer.fit(df).transform(df)

# COMMAND ----------

for c in data_types["LongType"]:
    df = df.withColumn(c+ "_cast_to_double", df[c].cast("double"))

cast_vars = [var for var in  df.columns if var.endswith("_cast_to_double")]
cast_vars_imputed  = [var+ "imputed" for var in cast_vars]

imputer_for_cast_vars = Imputer(inputCols = cast_vars, outputCols = cast_vars_imputed)
df = imputer_for_cast_vars.fit(df).transform(df)

# COMMAND ----------

## 전처리 한 데이터 조합
from pyspark.ml.feature import VectorAssembler

features = cast_vars_imputed + numericals_imputed \
          + [var + "_one_hot" for var in strings_used]

vector_assembler = VectorAssembler(inputCols = features, outputCol= "features")
data_training_and_test = vector_assembler.transform(df)

# COMMAND ----------

### 데이터 최종 검토 ###
data_training_and_test = data_training_and_test.join(df_join_cv.alias('temp'), data_training_and_test.tid == df_join_cv.tid, 'left')
data_training_and_test = data_training_and_test.drop("temp.tid")
data_training_and_test = data_training_and_test.na.fill(value=2.0, subset=["cv"])

display(data_training_and_test)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. 차원축소
# MAGIC * Feature 선택
# MAGIC * PCA(주성분분석)
# MAGIC * RandomForestClassioer

# COMMAND ----------

### 종속변수 확인
data_training_and_test.select(F.col('cv'), F.col('a.tid')).groupby('cv').agg(F.count('a.tid')).show()##.drop_duplicates().show()
# data_training_and_test.select(F.col('clk'), F.col('clk_string_encoded')).drop_duplicates().show()
# data_training_and_test.select(F.col('newUser'), F.col('newUser_string_encoded')).drop_duplicates().show()
# data_training_and_test.select(F.col('os'), F.col('os_string_encoded')).drop_duplicates().show()
# data_training_and_test.select(F.col('deviceType'), F.col('deviceType_string_encoded')).drop_duplicates().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## a. PCA

# COMMAND ----------

# data_training_and_test = data_training_and_test.drop("pca_features")
# data_training_and_test.printSchema()

# COMMAND ----------

from pyspark.ml.feature import PCA

# # here I Have defined maximum number of features that I have
pca_model = PCA(k = 10, inputCol = "features", outputCol = "pca_features")
# # fit the data to pca to make the model
pcaModel = pca_model.fit(data_training_and_test)
data_training_and_test = pcaModel.transform(data_training_and_test)

# here it will explain the variances
print(pcaModel.explainedVariance)
# get the cumulative values
cumValues = pcaModel.explainedVariance.cumsum()
# plot the graph 
plt.figure(figsize=(10,8))
plt.plot(range(1,11), cumValues, marker = 'o', linestyle='--')
plt.title('variance by components')
plt.xlabel('num of components')
plt.ylabel('cumulative explained variance')

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. RandomForestClassifier

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier

### train/test 데이터 분리
(training_data, test_data) = data_training_and_test.randomSplit([0.7, 0.3], 20230405)

### 모델 준비: 하이퍼파라미터 조절 필요
rf = RandomForestClassifier(labelCol = "cv", featuresCol = "features", numTrees = 20)
# rf = RandomForestClassifier(labelCol = "labels", featuresCol = "pca_features", numTrees = 20)

# COMMAND ----------

training_data.count()

# COMMAND ----------

training_data.select(F.col('cv'), F.col('a.tid')).groupby('cv').agg(F.count('a.tid')).show()

# COMMAND ----------

test_data.select(F.col('cv'), F.col('a.tid')).groupby('cv').agg(F.count('a.tid')).show()

# COMMAND ----------

### 모델 학습
rf_model = rf.fit(training_data)

### 모델 적합
md_predict = rf_model.transform(test_data)
# md_predict.toPandas().profile_report()

# COMMAND ----------

### 변수 중요도
print(rf_model.featureImportances)

# COMMAND ----------

from itertools import chain

list_featureImportances_attr = list(chain(*list(md_predict.schema["features"].metadata["ml_attr"]["attrs"].values())))
attrs = sorted((attr['idx'], attr['name']) for attr in list_featureImportances_attr)
list_featureImportances = [(name, rf_model.featureImportances[idx]) for idx, name in attrs if rf_model.featureImportances[idx]]
feature_importance = pd.DataFrame(list_featureImportances)
feature_importance.columns = ['names', 'importance']

# COMMAND ----------

display(feature_importance)

# COMMAND ----------

def plot_feature_importance(importance,names,model_type):

    #Create arrays from feature importance and feature names
    feature_importance = np.array(importance)
    feature_names = np.array(names)

    #Create a DataFrame using a Dictionary
    data={'feature_names':feature_names,'feature_importance':feature_importance}
    fi_df = pd.DataFrame(data)

    #Sort the DataFrame in order decreasing feature importance
    fi_df.sort_values(by=['feature_importance'], ascending=False,inplace=True)

    #Define size of bar plot
    plt.figure(figsize=(20,15))
    font = {'weight' : 'bold',
            'size'   : 15}
    matplotlib.rc('font', **font)
    
    #Plot Searborn bar chart
    sns.barplot(x=fi_df['feature_importance'], y=fi_df['feature_names'])
    #Add chart labels
    plt.title(model_type + 'FEATURE IMPORTANCE')
    plt.xlabel('FEATURE IMPORTANCE')
    plt.ylabel('FEATURE NAMES')

# COMMAND ----------

plot_feature_importance(feature_importance.importance, feature_importance.names, 'RANDOM FOREST')

# COMMAND ----------

md_predict.select(['a.tid', 'rawPrediction', 'probability', 'prediction']).show(5)

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="cv", predictionCol="prediction")
accuracy = evaluator.evaluate(md_predict)
print("Accuracy = %s" % (accuracy))
print("Test Error = %s" % (1.0 - accuracy))

# COMMAND ----------

