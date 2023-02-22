# Databricks notebook source
from datetime import datetime, timedelta
from pytz import timezone
from abc import ABC, abstractmethod

# COMMAND ----------

# logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
log_k = log4jLogger.LogManager.getLogger("Propfit-Streams")

# COMMAND ----------

now_time = datetime.now(timezone('Asia/Seoul'))
target_time = now_time - timedelta(hours = 1)
startTime = datetime.strftime(target_time, "%H:00:00")
endTime = datetime.strftime(now_time, "%H:00:00")

qurey_dict = {
    "cm_qurey" : f"""
    SELECT AdGroupIdx
    FROM   hive_metastore.ice.cm_adgroupschedule
    WHERE  CURRENT_DATE() LIKE `date`
        AND "{startTime}" like StartTime
        AND "{endTime}" like EndTime 
        AND GoalBudget > 0
    """
}

# COMMAND ----------

class streamsMonitor(ABC):
    """
    A class as Parent class inherited by streams eg. bronze_clk,imp,bid
    """
    
    def __init__(self) -> None:
        pass
    
    def proc_all(self) -> None:
        pass
      
    def data_load(self):
        pass
    
    def check_cm(self):
        pass
    
    def cm_camp_S3_check(self):
        pass
    
    def streams_validation(self):
        pass
    
    def cm_streams_validation(self):
        pass
      
    def s3_bronze_check(self):
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## monitoring bronze
# MAGIC 
# MAGIC ### usage
# MAGIC * cm에서 bidding 중일때, bronze table에 결과물이 적재되는지 여부를 확인
# MAGIC   * cm에서 bidding 중인 camp와 bronze에 적재되는 camp가 같은지?
# MAGIC   * 특정 시간에 S3에 쌓이는 row 수와 bronze table의 row 수가 같은지?
# MAGIC   
# MAGIC ### Remark
# MAGIC ~~S3에 쌓이는 row 수를 가져오는 `hive_metastore.cream.propfit_count_hourly` 이 매시 15분에 업데이트가 되므로, 15분 이후 1시간 이전 데이터를 체크해야 정상적으로 작동함.~~
# MAGIC 
# MAGIC S3에서 파일을 읽어와 직접 count하여, 작동 시간은 중요하지 않음

# COMMAND ----------

class bronzeMonitor(streamsMonitor):
    def __init__(self, table_list, qurey_dict, minutes=5) -> None:
        self.table_list = table_list
        self.database = "ice"
        
        self.now_time = datetime.now(timezone('Asia/Seoul'))
        self.target_time = self.now_time - timedelta(hours = 1)
        self.time_lag = datetime.strftime(self.target_time, "%Y-%m-%dT%H") + '%'
        self.s3_table = ""
        self.s3_df = ""
        self.bronze_df = ""
        self.qurey = qurey_dict
#         self.propfit_qurey = qurey_dict['propfit_qurey']
        
        
    def proc_all(self) -> None:
        """
        1. check cm
        2. check bronze : 1번이 되면
        3. check cm == bronze 
        """
#         self.qurey_maker()
        self.cm_df = self.data_load("cm")
        if self.check_cm(self.cm_df):
            for table in self.table_list:
                self.create_directory(table)
                self.qurey_maker(table)
                self.bronze_df, self.s3_df = self.data_load(table)
                
                if self.streams_validation(table, self.bronze_df):
                    self.s3_bronze_check(self.bronze_df, self.s3_df)
                    
                    if 'bid' in table:
                        self.cm_streams_validation(self.cm_df, self.bronze_df)
        else:
            return "not bidding..!!"
        
    def create_directory(self,table):
        self.s3_table = table.replace("bronze_", "")
        self.s3_directory = f"""dbfs:/mnt/ptbwa-basic/topics/{self.s3_table}/year={datetime.strftime(self.target_time, "%Y")}/month={datetime.strftime(self.target_time, "%m")}/day={datetime.strftime(self.target_time, "%d")}/hour={datetime.strftime(self.target_time, "%H")}/*"""
        
    def data_load(self, table="cm"):
        """
        data load cm or table
        """
        if table=="cm":
            return spark.sql(self.qurey["cm_qurey"]).toPandas()
        else:        
            bronze_df = spark.sql(self.qurey["bronze_qurey"]).toPandas()
            s3_df = spark.read.json(self.s3_directory)
            return bronze_df, s3_df
        
    def check_cm(self, cm_df):
        if cm_df.empty:
            print("not bidding...")
            return 0
        else:
            print("now bidding...")
            return 1
    
    # TODO utils 클래스를 하나 만들어서 SRP, validation체크 하는 로직은 static으로 불러보기
    def streams_validation(self, table, bronze_df):
        """
        streams bid,imp,clk 데이터들이 Delta Table에 제대로 적재 되는지 체크
        bronze의 actiontime_local과 현재 시간이 맞는지 체크 : 5분마다 디렉토리가 생성되므로 time lag의 default는 5분
        """
        self.table = table
        if bronze_df.empty:
            print(f"[FAIL-SAVE-{self.table}]")
            log_k.error(f"[FAIL-SAVE-{self.table}]")
            return 0
#             raise log_k.error(f"[FAIL-SAVE-{self.table}]")
        else:
            return 1
#             log_k.error(f"[GOOD-SAVE-{self.table}]")

    def cm_camp_S3_check(self):
        """
        현재 bidding 되고 있는 camp이 S3에 들어오고 있는지
        """
        pass
      
    def s3_bronze_check(self, bronze_df, s3_df):
        
        # TODO s3_df와 bornze_df의 ad_grp 별 count가 같은지 확인

        for b_ad_grp in bronze_df['ad_grp'].unique():
            b_ad_grp = int(b_ad_grp)
            if s3_df.filter(s3_df['BidStream']['ad_grp'] == b_ad_grp).count() == bronze_df.loc[bronze_df['ad_grp']==b_ad_grp,'count(bidstream.ad_grp)'].values[0]:
#                 log_k.error(f"[SAME-S3-{self.table}-CAMP-{b_ad_cam}]")
                pass
            else:
                log_k.error(f"[NOTSAME-NUM-S3-{self.table}-ADGROUP-{b_ad_grp}]")
            
    def cm_streams_validation(self, cm_df, bronze_df):
        """
        cm과 bronze의 CampaginIdx / AdGroupIdx 이 같은지 check
        """
        column_dict = {
#                         "CampaignIdx" : "ad_cam",
                       "AdGroupIdx" : "ad_grp"
                      }
        
        for key, value in column_dict.items():
            cm_idx_list = sorted(cm_df[key].unique().tolist())
            bronze_idx_list = sorted(bronze_df[value].unique().tolist())
        
            if cm_idx_list == bronze_idx_list:
                print(f"{key} Exactly Same")
                return 1
            else:
                print(f"Exactly not Same, {cm_idx_list}, {bronze_idx_list}")

#         for cm_camp in cm_camp_list:
#             for bronze_camp in bronze_camp_list:
#                 if cm_camp == bronze_camp:
#                     pass
            
    def qurey_maker(self,table=""):
        self.qurey = {
    "cm_qurey" : f"""
        SELECT AdGroupIdx
        FROM   hive_metastore.ice.cm_adgroupschedule
        WHERE  CURRENT_DATE() LIKE `date`
            AND "{startTime}" like StartTime
            AND "{endTime}" like EndTime 
            AND GoalBudget > 0
        """ ,
    "bronze_qurey" : f"""
        select bidstream.ad_grp,
           count(bidstream.ad_grp)
        from   {self.database}.{table}
        where  actiontime_local like '{self.time_lag}'
        group  by bidstream.ad_grp
    """
}

# COMMAND ----------

TABLE_LIST = ['streams_bid_bronze_app_nhn', 'streams_imp_bronze_app_nhn','streams_clk_bronze_app_nhn']
bronze = bronzeMonitor(TABLE_LIST, qurey_dict, 6)
bronze.proc_all()

# COMMAND ----------

