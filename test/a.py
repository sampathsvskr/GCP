##################################################
# Wheel Alignment Monitor Model and Dashboard Class
# Jose Torrado (jtorrad1@ford.com)
# Last Rev: 11/27/2023 (JT)
##################################################
import logging
import os
import datetime
import pandas as pd
import pyarrow
import numpy as np
import pyspark.sql
from sklearn import datasets, linear_model
from datetime import datetime,timedelta,date
from google.cloud import bigquery
from pyspark.sql import SparkSession,DataFrame
from pyspark import StorageLevel
import pyspark.sql.functions as f
from pyspark.sql.functions import col, desc, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType,IntegerType, DoubleType, TimestampType, FloatType, DateType
from pyspark import rdd


SCHEMA_FOR_TRAINING_OUTPUT_DF = StructType([
    StructField('vin', StringType()) ,
    StructField('model', StringType()) ,
    StructField('year', IntegerType()) ,
    StructField('flg_date', StringType()) ,    
    StructField('flag', StringType()), 
    StructField('flag_last', StringType()),
    StructField('trips_since_flag', IntegerType()),
    StructField('n_data', IntegerType()),
])

def map_to_df(spark: SparkSession, transformed_rdd: rdd):
    return spark.createDataFrame(transformed_rdd, SCHEMA_FOR_TRAINING_OUTPUT_DF)

class WAMBase:

    def __init__(self, config, env, spark, params_map) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = spark
        self.params_map = params_map
        self.config=config
        self.env= env

        
        #Initialize
        self.df_wam = None
        self.df_aws = None
        self.df_pae = None
        self.pd_vin = None
        self.df_swp = None

        #Congifurations
        self.version            =   config['WAM_CONFIG']['version']  
        self.pull_n_days        =   config['WAM_CONFIG']['pull_n_days']
        self.lrn_cnt_limit      =   config['WAM_CONFIG']['lrn_cnt_limit']
        self.npartitions        =   config['WAM_CONFIG']['Npartitions']
        self.region             =   config['WAM_CONFIG']['region']
        self.n_data_pull        =   config['WAM_CONFIG']['n_data_pull']
        self.save_bkt           =   config['WAM_CONFIG']['save_bkt']
        self.file_to_save       =   config['WAM_CONFIG']['file_to_save']
        self.cons_trip_reset    =   config['WAM_CONFIG']['cons_trip_reset']
        self.customer_table     =   config['WAM_CONFIG']['customer_table']
        self.cl                 =   config['WAM_CONFIG']['cl']
        
        # Lists to hold flagged VINs, etc.
        self.flg_list = None # List of VINs that are flagged as mis-aligned
        self.vin_list = None # List of VINs in TCU4G tables

        # BigQuery client
        self.client = bigquery.Client()

        #Getting them from BQ instead

        # fse_data_u553 = self.spark.read.format('bigquery').load("SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.FSE_List_U553`")
        # fse_data_p702 = self.spark.read.format('bigquery').load("SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.FSE_List_P702`")
        # vin_int_data = self.spark.read.format('bigquery').load("SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.VIN_List_Of_Interest`")

        fse_data_u553 = (
            self.spark.read.format("com.google.cloud.spark.bigquery")\
                        .option("query", "SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.FSE_List_U553`")\
                        .option('viewsEnabled', 'true')\
                        .option('materializationDataset','temp')\
                        .load()
        )

        fse_data_p702 = (
            self.spark.read.format("com.google.cloud.spark.bigquery")\
                        .option("query", "SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.FSE_List_P702`")\
                        .option('viewsEnabled', 'true')\
                        .option('materializationDataset','temp')\
                        .load()
        )

        vin_int_data = (
            self.spark.read.format("com.google.cloud.spark.bigquery")\
                        .option("query", "SELECT string_field_0 as vin FROM `ford-9cb842f9d96673f493b6aed5.vsu_staging.VIN_List_Of_Interest`")\
                        .option('viewsEnabled', 'true')\
                        .option('materializationDataset','temp')\
                        .load()
        )

        #To lists
        self.fse_list_u553 = list(fse_data_u553.select('vin').toPandas()['vin'])
        self.fse_list_p702 = list(fse_data_p702.select('vin').toPandas()['vin'])
        self.vin_int_list = list(vin_int_data.select('vin').toPandas()['vin'])


        print("DID RESPONSE PROJECT ID", config['DEV']['wam_did_response'],config['DEV']['wam_tcu4g'])
        if self.env == 'DEV':
            self.did_response =config['DEV']['wam_did_response']
            self.tcu4g = config['DEV']['wam_tcu4g']
        else:
            self.did_response =config['PROD']['wam_did_response']
            self.tcu4g= config['PROD']['wam_tcu4g']
    
    
    def __write_to_bigquery(self, df, table_name):
        
        df.write \
                .format("bigquery") \
                .mode("overwrite") \
                .option("temporaryGcsBucket", self.params_map["project_env_bucket"]) \
                .option("spark.datasource.bigquery.intermediateFormat", "orc") \
                .save(f"wam_dashboard.{table_name}")
    
    def bq_query_read(self,query) -> list[pyspark.sql.DataFrame]:

        self.logger.info('FindTruePositives >>> Starting AWS query, Starting S360 query, Starting VIN timeseries query ')

        df=[]
        for i in query:
            df.append(self.spark.read.format('bigquery').load(i))
        return df


    def fetch_swp(self):
        '''
        Method to pull software part numbers for WAM. 
        '''        

        myfilter  = ','.join(["'" +i + "'" for i in  self.fse_list_u553+self.fse_list_p702+self.vin_int_list])

        query =  """SELECT 
                        scvckq_did_response  AS part,
                        scvckq_vin as vin, 
                        scvckq_created_date as part_date
                      FROM 
                        `{}.bq_47_did_response_fdp_dwc_vw.nscvckq_did_response_latest_vw`
                      WHERE 
                      scvckq_created_date >= '2023-01-01' and
                      scvckq_ecu_address = '0X0716' and
                      scvckq_vin_country = 'USA' and
                      scvckq_did ='F188' and
                      scvckq_vin in ({})
        """.format(self.did_response,myfilter)
        print(query)
        self.logger.info('FetchSWP >>> Begin query')
        self.df_swp = self.spark.read \
            .format('bigquery') \
            .option('query', query) \
            .load()

        if self.df_swp.count()==0:
            self.logger.warning('FetchSWP >>> No data fetched')
        else:
            self.logger.info('FetchSWP >>> Data fetched successfully')

        self.logger.info('FetchSWP >>> Write to BQ')
        self.__write_to_bigquery(self.df_swp, 'strat_part_number')
        self.logger.info('FetchSWP >>> Done')

    def fetch_tcu4g(self):
        '''
        Method to pull TCU4G data for WAM. 
        '''
        self.logger.info('FetchTCU4G >>> Begin')

        pull_range = (datetime.today() - timedelta(days=int(self.pull_n_days))).strftime('%Y-%m-%d')

        serv360_table='prj-dfdm-95-vs360-p-95.bq_dm_vsr_s360_fdp_dmc_vw.vsrveh001_veh_mstr_vw'

        query =  """
                    SELECT 
                        T3.*,
                        T4.whl_base_ds as wheelbase
                    FROM (
                        SELECT 
                            T1.cvdc62_vin_d_3 AS vin, 
                            T1.cvdc62_model_x_3 as veh_model, 
                            T1.cvdc62_year_r_3 as year, 
                            T1.cvdc62_authmode_x_3 as auth1, 
                            T1.cvdc62_auth_stat_c as auth2, 
                            signals.odometermastervalue as odo, 
                            signals.externalvariablehitcount1 as hitcount1,
                            signals.externalvariablehitcount2 as hitcount2, 
                            signals.externalvariablesignalstatus as sig_status,
                            signals.externalvariablestepinflg as stepin_flg,
                            signals.wamexternalvariabledata_stepinoffsetv2 as stepin_offset,
                            signals.internalvariabledrivestate as drive_state,
                            signals.internalvariablestepinoffsetinit as stepin_offset_init,
                            signals.wheelmonitorsamplingtype as samp_typ,
                            signals.wheelmonitortimestamp as timestamp,
                            signals.internalvariablecntrlrn as cntrlrn,
                            signals.wheelmonitorutcoffset as utc_offset,
                            signals.internalvariablerollangle as roll_angle,
                            signals.internalvariablevehlatcompaest as latt_comp,
                            signals.internalvariablevehlatcomperr as latt_comp_er
                        FROM 
                            `{}.bq_34_tcu4g_feature_fdp_dwc_vw.ncvdc62_fnv2_misc_vw` T1, 
                            UNNEST(T1.cvdc62_wheelalignmentdata_x) as signals 
                        WHERE 
                            T1.cvdc62_msg_metadata_msg_n = 'WAMDataAlert'
                            AND T1.cvdc62_partition_country_x = {}
                            AND T1.df_partition_date >= '{}'     
                    ) T3 
                    LEFT JOIN (
                        SELECT vin_nb, whl_base_ds
                        FROM `{}`
                    ) T4
                    ON T3.vin = T4.vin_nb
                """.format(self.tcu4g ,self.region, pull_range, serv360_table)
     
        myfilter  = ','.join(["'" +i + "'" for i in  self.fse_list_u553+self.fse_list_p702+self.vin_int_list])
        query1 =  """
                     SELECT 
                        T3.*,
                        T4.whl_base_ds as wheelbase
                    FROM (
                    SELECT 
                        T1.cvdc62_vin_d_3 AS vin, 
                        T1.cvdc62_model_x_3 as veh_model, 
                        T1.cvdc62_year_r_3 as year, 
                        T1.cvdc62_authmode_x_3 as auth1, 
                        T1.cvdc62_auth_stat_c as auth2, 
                        signals.odometermastervalue as odo, 
                        signals.externalvariablehitcount1 as hitcount1,
                        signals.externalvariablehitcount2 as hitcount2, 
                        signals.externalvariablesignalstatus as sig_status,
                        signals.externalvariablestepinflg as stepin_flg,
                        signals.wamexternalvariabledata_stepinoffsetv2 as stepin_offset,
                        signals.internalvariabledrivestate as drive_state,
                        signals.internalvariablestepinoffsetinit as stepin_offset_init,
                        signals.wheelmonitorsamplingtype as samp_typ,
                        signals.wheelmonitortimestamp as timestamp,
                        signals.internalvariablecntrlrn as cntrlrn,
                        signals.wheelmonitorutcoffset as utc_offset,
                        signals.internalvariablerollangle as roll_angle,
                        signals.internalvariablevehlatcompaest as latt_comp,
                        signals.internalvariablevehlatcomperr as latt_comp_er
                    FROM 
                        `{}.bq_34_tcu4g_feature_fdp_dwc_vw.ncvdc62_fnv2_psa_vw` T1, UNNEST(T1.cvdc62_wheelalignmentdata_x) as signals 
                    WHERE 
                        T1.cvdc62_msg_metadata_msg_n='WAMDataAlert'
                        AND T1.cvdc62_partition_country_x={}
                        AND T1.df_partition_date >= '{}'
                        AND T1.cvdc62_fcs_flag_x = 'Y'
                        AND T1.cvdc62_vin_d_3 in ({})
                    ) T3 
                    LEFT JOIN (
                        SELECT vin_nb, whl_base_ds
                        FROM `{}`
                    ) T4
                    ON T3.vin = T4.vin_nb
                """.format(self.tcu4g, self.region, pull_range,myfilter, serv360_table)
        
        """adding this as we have table wam_base """

        query= "select * from `ford-d7910739734ba7b122bc0c0b.wam_dashboard.wam_base`"

        
        self.logger.info('FetchTCU4G >>> Starting Query')
        
        self.df_wam  = self.spark.read.format('bigquery').load(query)\
            .union(self.spark.read.format('bigquery').load(query1))
        
        self.logger.info('FetchTCU4G >>> Finished Query')
        
        #Filter by FSE VINs and authorized retail, combine in end
        df_wam_fil_fse1 = self.df_wam.filter(self.df_wam.vin.isin(self.fse_list_p702))
        df_wam_fil_fse = self.df_wam.filter(self.df_wam.vin.isin(self.fse_list_u553))
        
        df_wam_fil_auth = self.df_wam.filter(self.df_wam.auth2.isin('AUTHORIZED'))

        df_wam_fil_fse1 = df_wam_fil_fse1.withColumn("FSE_Flag",f.lit("2"))
        df_wam_fil_fse = df_wam_fil_fse.withColumn("FSE_Flag",f.lit("1"))
        df_wam_fil_auth = df_wam_fil_auth.withColumn("FSE_Flag",f.lit("0")) # authentication filtering by default removes FSE
        
        df_wam = df_wam_fil_auth.union(df_wam_fil_fse).union(df_wam_fil_fse1)

        self.logger.info('FetchTCU4G >>> End')
        return df_wam
        
        
        
    def fill_monitor(self, df_wam):
        '''
        Method to fill CSV files for WAM dashboard
        '''
        
        self.logger.info('FillMonitor >>> Begin')
        
        df_max = df_wam.groupBy('vin').agg(
                        f.max('veh_model').alias('veh_model'),
                        f.max('FSE_Flag').alias('FSE_Flag'),
                        f.max('wheelbase').alias('wheelbase'),                    
                        f.max('cntrlrn').alias('cntrlrn'),
                        f.max('year').alias('year'),                              
                        f.max('stepin_offset').alias('max_stepin_offset'),
                        f.max('stepin_flg').alias('max_stepin_flg'),                                                                 
                        f.max('stepin_offset_init').alias('max_stepin_offset_init'),
                        f.max('hitcount1').alias('max_hitcount1'),
                        f.max('hitcount2').alias('max_hitcount2'),
                        f.max('sig_status').alias('max_sig_status'), 
                        f.max('drive_state').alias('max_drive_state'),
                        f.min('stepin_offset').alias('min_stepin_offset'),
                        f.min('stepin_flg').alias('min_stepin_flg'),
                        f.min('stepin_offset_init').alias('min_stepin_offset_init'),
                        f.min('hitcount1').alias('min_hitcount1'),
                        f.min('hitcount2').alias('min_hitcount2'),
                        f.min('sig_status').alias('min_sig_status'), 
                        f.min('drive_state').alias('min_drive_state'), 
                        f.avg('stepin_offset').alias('avg_stepin_offset'),
                        f.avg('stepin_flg').alias('avg_stepin_flg'),
                        f.avg('stepin_offset_init').alias('avg_stepin_offset_init'),
                        f.avg('hitcount1').alias('avg_hitcount1'),
                        f.avg('hitcount2').alias('avg_hitcount2'),
                        f.avg('sig_status').alias('avg_sig_status'), 
                        f.avg('drive_state').alias('avg_drive_state'),
                        f.countDistinct('timestamp').alias('Nentries')
                                                                     )

        # Doing transforms/filter in spark
        df_max = df_max.withColumn('max_amb_temp', f.lit(0))
        df_max = df_max.withColumn('min_amb_temp', f.lit(0))
        df_max = df_max.withColumn('ave_amb_temp', f.lit(0))
        df_max = df_max.withColumn('veh_model', f.regexp_replace(f.col('veh_model'), 'FORD',''))
        df_max = df_max.withColumn('veh_model', f.regexp_replace(f.col('veh_model'), "^ +| +$", ""))
        
        # Hardcoded for DEV right now since the table is just available there
        ecg_ex_level_query = """
        SELECT 
            scvckq_vin as vin,
            ex_level_lts,
            pn_F113_lts,
            pn_8033_lts,
            pn_F188_lts,
        FROM `ford-666e9ca1b9bb35e73424cf75.vsu_main.ecg_ex_fpu_level`
        """

        ecg_ex_level = self.spark.read.format('bigquery').load(ecg_ex_level_query)

        df_max = df_max.join(ecg_ex_level, 'vin', 'left')
        
        
        self.logger.info('FillMonitor >>> WAM Writing to BQ')
        self.__write_to_bigquery(df_max, "wam_dashboard")
        
        self.logger.info('FillMonitor >>> Flg portion')
        
        w = Window.partitionBy('vin').orderBy("timestamp")
        w1 = Window.partitionBy('vin').orderBy(desc("timestamp"))
        df_wam_flg = df_wam.filter(df_wam.stepin_flg.isin(['1','2']))
        df_wam_flg = df_wam_flg.orderBy('timestamp',ascending=True)

        df_wam_flg = df_wam_flg.withColumn("rn", f.row_number().over(w)).filter("rn = 1").drop("rn")
        df_flg = df_wam_flg.groupBy(['vin']).agg(f.max('timestamp').alias('flg_timestamp'))

        self.logger.info('FillMonitor >>> Saving FLg to BQ Table')
        self.__write_to_bigquery(df_flg, "wam_flg")
        
        self.logger.info('FillMonitor >>> End Flg portion')
        
        self.logger.info('FillMonitor >>> Latest portion')
        
        df_wam_latest = self.df_wam.orderBy('timestamp',ascending=False)
        df_wam_latest = df_wam_latest.withColumn("rn", f.row_number().over(w1)).filter("rn = 1").drop("rn")
        df_latest = df_wam_latest.groupBy('vin').agg(f.max('stepin_flg').alias('flg_latest'))
        
        self.logger.info('FillMonitor >>> Saving latest to BQ')
        self.__write_to_bigquery(df_latest, "wam_latest")
        
        self.logger.info('FillMonitor >>> End latest portion')
        
        pd_wam = df_max.toPandas()
        self.flg_list = pd_wam[pd_wam['max_stepin_flg'].isin([1,2])]['vin'].unique().tolist()
        self.vin_list = pd_wam['vin'].unique().tolist()

        self.logger.info('FillMonitor >>> End')
        
    def fill_time_series(self, df_wam):
        
        self.logger.info('FetchTimeSeries >>> Begin')
        full_vin_int_list = self.vin_int_list + self.fse_list_p702 + self.fse_list_u553 + self.flg_list
        
        df_vin = df_wam.filter(df_wam.vin.isin(full_vin_int_list))
        df_vin = df_vin.repartition('vin')


        pd_vin = df_vin.groupBy('vin','timestamp').agg(
                        f.max('cntrlrn').alias('cntrlrn'),
                        f.max('stepin_offset').alias('stepin_offset'),
                        f.max('stepin_offset_init').alias('stepin_offset_init'),                    
                        f.max('stepin_flg').alias('stepin_flg'),
                        f.max('hitcount1').alias('hitcount1'),
                        f.max('hitcount2').alias('hitcount2'),                              
                        f.max('drive_state').alias('drive_state'),                                                                 
                        f.max('sig_status').alias('sig_status'),
                        f.max('FSE_Flag').alias('FSE_Flag'),
                        f.max('roll_angle').alias('roll_angle'), 
                        f.max('latt_comp').alias('latt_comp'),
                        f.max('latt_comp_er').alias('latt_comp_er'),
                        f.max('samp_typ').alias('samp_typ'))

        pd_vin = pd_vin.withColumn('stepin_flg', col('stepin_flg').cast(IntegerType()))
        pd_vin = pd_vin.withColumn('hitcount1', col('hitcount1').cast(IntegerType()))
        pd_vin = pd_vin.withColumn('hitcount2', col('hitcount2').cast(IntegerType()))
        pd_vin = pd_vin.withColumn('sig_status', col('sig_status').cast(IntegerType()))

        pd_vin = pd_vin.orderBy('timestamp')
        pd_vin = pd_vin.filter(col('timestamp') > '2021-01-01 00:00:00') # needed for some anomalous dates from 2008(?)

        if(pd_vin.count()>0):
            self.logger.info('FetchTimeSeries >>> First 10 Rcords', pd_vin.show(10))
        else:
            self.logger.info('FetchTimeSeries >>> no Rcords', pd_vin.count())

        # self.logger.info('FetchTimeSeries >>> Schema', pd_vin.schema)

        self.logger.info('FetchTimeSeries >>> Writing to BQ')

        self.__write_to_bigquery(pd_vin, "vin_time")
        
        self.logger.info('FetchTimeSeries >>> End')
                 
     
    def run_model_vin_group(self,vin_main_df):

        ### Hard coded model parameters ######################################################
        hit_cnt1_thresh = 30 # Threshold for hit counter 1
        hit_cnt2_thresh = 30 # Threshold for hit counter 2
        hit_cnt1_max = 1 * hit_cnt1_thresh # Max for hit counter 1
        hit_cnt2_max = 1 * hit_cnt2_thresh # Max for hit counter 2     
        npoints = 100 # Number of data points to user in each model run
        
        ######################################################################################
        
        try:

            #vin_main_df = vin_partition.toDF()
            SCHEMA_FOR_TRAINING_OUTPUT_DF = StructType([
                StructField('vin', StringType()) ,
                StructField('model', StringType()) ,
                StructField('year', IntegerType()) ,
                StructField('date', DateType()) ,
                StructField('flg_date', StringType()) ,    
                StructField('flag', StringType()), 
                StructField('flag_last', StringType()),
                StructField('trips_since_flag', IntegerType()),
                StructField('n_data', IntegerType()),
            ])
            model_df = self.spark.createDataFrame([],SCHEMA_FOR_TRAINING_OUTPUT_DF)

            vins = vin_main_df.select('vin').distinct().collect()

            ## Getting the count of rows for each vin
            vin_count_df = vin_main_df.select('vin').groupBy('vin').count().filter('count>=100').drop('count')

            # Discarding vins which has data points < 100
            vin_main_df = vin_main_df.join(vin_count_df,on='vin',how='inner')
            

            for row in vins:
             
                vv = row.vin
                vin_df = vin_main_df.filter(f"vin = '{vv}'")
                
                flag_current = 1 # start on flag 1: no mis-alignment
                flag_last = 1 # start on flag 1: no mis-alignment
                total = vin_df.count()

                vin_df = vin_df.withColumn('timestamp',f.to_timestamp('timestamp')) \
                                .orderBy('timestamp')
                
                vin_data = vin_df.take(1)[0]
                m_model = vin_data['veh_model']
                if not m_model:
                    m_model = 'null'
                
                m_year = vin_data['year']
                try:
                    m_year = int(m_year)
                except ValueError:
                    m_year = 1900

                m_date = vin_data['date']
                if not m_date:
                    m_date = '2020-01-01'
                m_flg_date = []    
                m_flg = [] 
                m_flg_last = []
                m_t_flag = 0

                vin_df = vin_df.withColumn('just_date',f.to_date('timestamp'))
                
                day_list = vin_df.select('just_date').distinct().collect()
                
                #Current vehicles information, if any flag was set previously
                flag = vin_data['flag']
                if flag is not None:
                    vin_df['flag'] = pd.to_numeric(vin_df['flag']).astype(float)
                    vin_df = vin_df.withColumn('flag',f.col('falg').cast(FloatType()))
                    if float(flag) > 1:
                        flag_current = flag
                        m_t_flag = vin_data['trips_since_flag']
                        vin_df = vin_df.filter('timestamp > flg_date')
                        
                
                for day in day_list:
                    day = day.just_date
                    
                    df_tmp = vin_df.filter(f"just_date <= '{day}'")
                    
                    if df_tmp.count() < npoints:
                        continue
                    
                    df_tmp = df_tmp.orderBy('timestamp')
                    df_tmp = df_tmp.limit(npoints)
                    
                    ################################## Model Logic #####################################
                    
                    df_tmp_data = df_tmp.take(1)[0]
                    flag_end = df_tmp_data['stepin_flg']
                    current_date = df_tmp_data['just_date']

                    window_spec = Window.orderBy("timestamp")
                    df_tmp = df_tmp.withColumn("diff_hc1", f.col("hitcount1") - f.lag("hitcount1", 1).over(window_spec)) \
                                    .withColumn("diff_hc2", f.col("hitcount2") - f.lag("hitcount2", 1).over(window_spec))
                    
                    
                    hc1_maxed = df_tmp.filter(f"hitcount1 == {hit_cnt1_max}").count()
                    hc2_maxed = df_tmp.filter(f"hitcount2 == {hit_cnt2_max}").count()
                    hc1_up = df_tmp.filter("diff_hc1 > 0").count()
                    hc2_up = df_tmp.filter("diff_hc2 > 0").count()
                    
                    hc1_isallzero = False
                    hc2_isallzero = False
                    hc1_isincreasing = False
                    hc2_isincreasing = False  
                    hc1_isallmax = False
                    hc2_isallmax = False
                    
                    if df_tmp.filter("diff_hc1 = 0").count() == npoints:
                         hc1_isallzero = True
                            
                    if df_tmp.filter("diff_hc2 = 0").count() == npoints:
                         hc1_isallzero = True   

                    if hc1_up > npoints/2.:
                        hc1_isincreasing = True

                    if hc2_up > npoints/2.:
                        hc2_isincreasing = True
                            
                    if (hc1_maxed + hc1_up - 1) > npoints/2.: # minus one need to avoid double counting hitting max
                        hc1_isincreasing = True

                    if (hc2_maxed + hc2_up - 1) > npoints/2.: # minus one need to avoid double counting hitting max
                        hc2_isincreasing = True
                    
                    if hc1_maxed == npoints:
                        hc1_isallmax = True
                            
                    if hc2_maxed == npoints:
                        hc2_isallmax = True    
                    
                    ### Logic part
                    
                    if flag_end == 0:
                            
                            if (hc1_isallzero or not hc1_isincreasing) and (hc2_isallzero or not hc2_isincreasing):
                                flag_current = 1 # No mis-alignment
                            else:
                                flag_current = 1.1 # No mis-alignment, low confidence
                                
                    elif flag_end == 1:
                        
                            if (hc1_isincreasing or hc1_isallmax) and (hc2_isallzero or not hc2_isincreasing):
                                flag_current = 2 # Moderate mis-alignment, high confidence
                            elif not hc1_isincreasing:
                                flag_current = 2.1 # Moderate mis-alignment, low confidence
                            else:
                                flag_current = 3.1 # Severe mis-alignment, low confidence
                    
                    elif flag_end == 2:
                        
                            if (hc1_isincreasing or hc1_isallmax) and (hc2_isincreasing or hc2_isallmax):
                                flag_current = 3 # Severe mis-alignment, high confidence
                            elif (hc1_isincreasing or hc1_isallmax) and not hc2_isincreasing:
                                flag_current = 2.2 # Moderate mis-alignment, high confidence
                            else:
                                flag_current = 1.2 # No mis-alignment, low confidence
                    
                    ###
                    
                    if flag_current != flag_last: # only append data if flag changes states
                       
                        m_flg.append(flag_current)
                        m_flg_date.append(current_date)
                        m_flg_last.append(flag_last)      
                        m_t_flag = 0
                        flag_last = flag_current                            
                            
                    else: # Current flag counter
                        m_t_flag += 1
                        
                    ################################## End Model #################################
                
                ### Only keep vehicles with any flag
                if len(m_flg) > 0:
                    
                    result = {"vin": str(vv),
                              "model": str(m_model),
                              "year": int(m_year),
                              "date": str(m_date),
                              "flg_date": ','.join(str(x) for x in m_flg_date),
                              "flag": ','.join(str(x) for x in m_flg),
                              "flag_last": ','.join(str(x) for x in m_flg_last),                          
                              "trips_since_flag": int(m_t_flag),
                              "n_data": int(total)
                    }
                    
                    model_df = model_df.union(WAMBase.spark.createDataFrame([result]))
                    
                    
            return model_df

        except Exception as error:
            logging.exception("Error occurred in run_model_vin_group function: \n{}".format(error))
            
                            
    def pre_process(self):
        '''
        Method to preprocess data for PAE
        '''
        
        self.logger.info('PreProcess >>> Begin')
        pull_range = (datetime.today() - timedelta(days=int(self.pull_n_days))).strftime('%Y-%m-%d')
        
        query =  """SELECT * 
                    FROM
                    (
                      SELECT 
                        cvdc62_vin_d_3 AS vin,
                        df_partition_date as date, 
                        cvdc62_model_x_3 as veh_model, 
                        cvdc62_year_r_3 as year,
                        signals.wheelmonitortimestamp as timestamp,
                        signals.externalvariablehitcount1 as hitcount1,
                        signals.externalvariablehitcount2 as hitcount2, 
                        signals.externalvariablestepinflg as stepin_flg,
                        row_number()
                        OVER
                        (
                          PARTITION BY 
                            cvdc62_vin_d_3
                          ORDER BY
                            df_partition_date desc
                        ) as my_rank
                      FROM 
                         `{}.bq_34_tcu4g_feature_fdp_dwc_vw.ncvdc62_fnv2_misc_vw`, 
                         UNNEST(cvdc62_wheelalignmentdata_x) as signals 
                      WHERE 
                        cvdc62_msg_metadata_msg_n = 'WAMDataAlert'
                        AND cvdc62_partition_country_x = {}
                        AND df_partition_date >= '{}'
                        AND signals.wheelmonitorsamplingtype IN ('PERIODIC', 'IGNITION_OFF')
                        AND signals.internalvariabledrivestate = 7
                        AND signals.externalvariablesignalstatus = 127
                        AND signals.internalvariablecntrlrn >= {}
                        AND cvdc62_auth_stat_c = 'AUTHORIZED'
                        AND cvdc62_authmode_x_3 = 'Retail'
                    ) ranks 
                    WHERE my_rank <= {}
                """.format(self.tcu4g,self.region,pull_range,self.lrn_cnt_limit,self.n_data_pull)

        query1 =  """SELECT * 
                    FROM
                    (
                      SELECT 
                        cvdc62_vin_d_3 AS vin,
                        df_partition_date as date, 
                        cvdc62_model_x_3 as veh_model, 
                        cvdc62_year_r_3 as year, 
                        signals.wheelmonitortimestamp as timestamp,
                        signals.externalvariablehitcount1 as hitcount1,
                        signals.externalvariablehitcount2 as hitcount2, 
                        signals.externalvariablestepinflg as stepin_flg,
                        row_number()
                        OVER
                        (
                          PARTITION BY 
                            cvdc62_vin_d_3
                          ORDER BY
                            df_partition_date desc
                        ) as my_rank
                      FROM 
                         `{}.bq_34_tcu4g_feature_fdp_dwc_vw.ncvdc62_fnv2_psa_vw`, 
                         UNNEST(cvdc62_wheelalignmentdata_x) as signals 
                      WHERE 
                        cvdc62_msg_metadata_msg_n = 'WAMDataAlert'
                        AND cvdc62_partition_country_x = {}
                        AND df_partition_date >= '{}'
                        AND signals.wheelmonitorsamplingtype IN ('PERIODIC', 'IGNITION_OFF')
                        AND signals.internalvariabledrivestate = 7
                        AND signals.externalvariablesignalstatus = 127
                        AND signals.internalvariablecntrlrn >= {}
                        AND cvdc62_fcs_flag_x = 'Y'
                    ) ranks 
                    WHERE my_rank <= {}
                """.format(self.tcu4g, self.region,pull_range,self.lrn_cnt_limit,self.n_data_pull)
        
        self.logger.info('PreProcess >>> Starting preprocess query')
        
        self.df_pae  = self.spark.read.format('bigquery').load(query).union(self.spark.read.format('bigquery').load(query1))
        self.df_pae = self.df_pae.drop('my_rank')
        self.logger.info('PreProcess >>> End')
        
    def run_model(self):
        
        self.logger.info('RunModel >>> Begin')
        
        num_of_vins_received = self.df_pae.select('vin').distinct().count()
        num_of_records_received = self.df_pae.count()
       
        query = """ select 
                        vin, 
                        flag,
                        flg_date,
                        trips_since_flag
                    FROM (
                        SELECT *,
                        row_number()
                        OVER
                        (
                          PARTITION BY 
                            vin
                          ORDER BY
                            flg_date desc
                        ) as my_rank
                        FROM
                        {}
                        ) ranks
                        WHERE my_rank < 2
                """.format(self.customer_table)  
        
        self.logger.info('RunModel >>> Starting customer table query')
        
        spark_df = self.spark.read.format('bigquery').load(query)
        if spark_df.count() == 0:
            self.logger.warning('RunModel >>> No records found in customer table. Skipping join.')
        else:
            spark_df = spark_df.drop('my_rank')
            self.df_pae = self.df_pae.join(spark_df,'vin','left')
        # self.df_pae.printSchema()
        
        vin_partitioned_dataframe = self.df_pae.repartition( 'vin')

        self.logger.info('RunModel >>> Running model on partitioned dataframe')
        
        model_output_df = self.run_model_vin_group(vin_partitioned_dataframe)

        self.logger.info('RunModel >>> Mapping output dataframe')
        #model_output_df = map_to_df(self.spark, model_output)
        # model_output_df.printSchema()
        
        num_of_vins_trained = model_output_df.select('vin').distinct().count()
        num_of_records_trained = model_output_df.count()
        
        # model_output_df.printSchema()
        df_save = model_output_df.toPandas()

        df_save['version'] = str(self.version)
        df_save['model'] = df_save['model'].map(lambda x: x and x.replace('FORD',''))
        df_save['model'] = df_save['model'].replace(r"^ +| +$", r"", regex=True)
        
        df_save['flg_date'] = df_save['flg_date'].apply(lambda x: list(x.split(',')))
        df_save['flag'] = df_save['flag'].apply(lambda x: list(x.split(',')))
        df_save = df_save.explode(['flag','flg_date'])
        df_save['flg_date'] = pd.to_datetime(df_save['flg_date'])
        self.logger.info('RunModel >>> Saving Customer Logic Results To BigQuery')
        
        job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("vin", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("version", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("model", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("flg_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("flag", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("flag_last", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("trips_since_flag", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("n_data", bigquery.enums.SqlTypeNames.INTEGER),
        ],
        write_disposition="WRITE_TRUNCATE",)

        job = self.client.load_table_from_dataframe(
        df_save, self.customer_table, job_config=job_config
        )
        job.result()
        
        self.logger.info("Run Model >>> Statistics:")
        self.logger.info("Run Model >>> num_of_vins_received >>>",num_of_vins_received)
        self.logger.info("Run Model >>> num_of_records_received >>>",num_of_records_received)
        self.logger.info("Run Model >>> num_of_vins_trained >>>",num_of_vins_trained)
        self.logger.info("Run Model >>> num_of_records_trained >>>",num_of_records_trained)
        
        self.logger.info('RunModel >>> End')

    def get_ro(self):
        self.logger.info('GetRO >>> Begin')

        #Service 360
        self.logger.info('GetRO >>> Service360')

        serv360_table1='prj-dfdm-95-vs360-p-95.bq_dm_vsr_s360_fdp_dmc_vw.vsrgro302_clsd_ro_srvc_vw'
        serv360_table2='prj-dfdm-95-vs360-p-95.bq_dm_vsr_s360_fdp_dmc_vw.vsrgro301_clsd_ro_hdr_vw'

        query_gudb = """
        SELECT T1.srvc_orig_lbr_oper_cd,
            T1.srvc_lbr_oper_cd,
            T1.srvc_lbr_ds,
            T1.ro_open_dt,
            T1.ro_srvc_clsd_dt,
            T2.CUST_TYP_OF_SALE_CD,
            T2.VIN_NB
        FROM (SELECT *
            FROM `{}`
            WHERE ro_srvc_clsd_dt > '2022-04-01'
                AND srvc_rtl_actn_rpr_catg_cd IN ('04')
                AND (srvc_lbr_ds LIKE '%align%' OR srvc_lbr_ds LIKE '%Align%' OR srvc_lbr_ds LIKE '%ALIGN%')
                AND (srvc_lbr_ds LIKE '%wheel%' OR srvc_lbr_ds LIKE '%Wheel%' OR srvc_lbr_ds LIKE '%WHEEL%')
            ) T1
        JOIN `{}` T2
        ON T1.ro_id_nb = T2.ro_id_nb
            AND T1.paacct_cd = T2.paacct_cd
            AND T1.ro_open_dt = T2.ro_open_dt
        WHERE T2.VIN_MDL_YR > '2021'
        """.format(serv360_table1, serv360_table2)

        df_gudb = self.client.query(query_gudb).to_dataframe()
        
        self.logger.info(f"GetRO >>> check datatype of df_gudb:\n{df_gudb.dtypes}")

        df_gudb = df_gudb.drop_duplicates(['VIN_NB','srvc_lbr_ds','ro_srvc_clsd_dt'])

        df_gudb1 = df_gudb[df_gudb['VIN_NB'].isin(self.vin_list)]
        df_gudb1 = df_gudb1[(df_gudb1['srvc_lbr_ds'].str.contains('Wheel')|df_gudb1['srvc_lbr_ds'].str.contains('WHEEL')|df_gudb1['srvc_lbr_ds'].str.contains('wheel'))]
        
        job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("srvc_orig_lbr_oper_cd", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("srvc_lbr_oper_cd", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("srvc_lbr_ds", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("ro_open_dt", bigquery.enums.SqlTypeNames.DATE),
                    bigquery.SchemaField("ro_srvc_clsd_dt", bigquery.enums.SqlTypeNames.DATE),
                    bigquery.SchemaField("CUST_TYP_OF_SALE_CD", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("VIN_NB", bigquery.enums.SqlTypeNames.STRING),
                ],
                write_disposition="WRITE_TRUNCATE",)
        
        self.logger.info('GetRO >>> Service360 to BQ')
        job = self.client.load_table_from_dataframe(df_gudb1, 'wam_dashboard.s360_rpr', job_config=job_config)
        job.result()

        # AWS
        self.logger.info('GetRO >>> AWS')
        query = '''
            SELECT 
                vin_cd as vin,cust_conc_cd,rpr_dt
            FROM 
                `prj-dfdl-625-aws-p-625.bq_625_aws_lnd_lc_vw.clm_21_vw`
            WHERE 
                 cust_conc_cd in ('C54','H24','H62')
            AND rpr_dt >= '2023-01-01'
            AND vin_cd is not null
        '''
        query1 = '''
                    SELECT 
                        vin_cd as vin,cust_conc_cd,rpr_dt
                    FROM 
                        `prj-dfdl-625-aws-p-625.bq_625_aws_lnd_lc_vw.clm_22_vw`
                    WHERE 
                        cust_conc_cd in ('C54','H24','H62')
                    AND rpr_dt >= '2023-01-01'
                    AND vin_cd is not null
        '''
        query2 = '''
                    SELECT 
                        vin_cd as vin,cust_conc_cd,rpr_dt
                    FROM 
                        `prj-dfdl-625-aws-p-625.bq_625_aws_lnd_lc_vw.clm_23_vw`
                    WHERE 
                        cust_conc_cd in ('C54','H24','H62')
                    AND rpr_dt >= '2023-01-01'
                    AND vin_cd is not null
        '''

        df_aws  = self.client.query(query).to_dataframe()
        df_aws1  = self.client.query(query1).to_dataframe()
        df_aws2  = self.client.query(query2).to_dataframe()

        df_aws = df_aws.append(df_aws1).append(df_aws2)
        df_aws = df_aws[df_aws['vin'].isin(self.vin_list)]
        df_aws = df_aws.drop_duplicates(['vin','rpr_dt','cust_conc_cd'])
        df_aws['rpr_dt'] = pd.to_datetime(df_aws['rpr_dt'],utc=False)

        job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("vin", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("cust_conc_cd", bigquery.enums.SqlTypeNames.STRING),
                    bigquery.SchemaField("rpr_dt", bigquery.enums.SqlTypeNames.TIMESTAMP),
                ],
                write_disposition="WRITE_TRUNCATE",)
        
        
        job = self.client.load_table_from_dataframe(df_aws, 'wam_dashboard.aws_rpr', job_config=job_config)
        job.result()

        self.logger.info('GetRO >>> End')

    
    def find_true_positives_transform(self, df_aws, df_s360, pd_time):
        min_date = pd_time.select("timestamp").rdd.min()[0]
        max_date = pd_time.select("timestamp").rdd.max()[0]

        self.logger.info(f'FindTruePositives >>> MinDate:\n{min_date}')
        self.logger.info(f'FindTruePositives >>> MaxDate:\n{max_date}')
        self.logger.info('FindTruePositives >>> Starting VIN transformation')
 
        # Create pd_time lookup table for S360 and AWS dataframes to join
        window = Window.partitionBy('vin').orderBy(col("timestamp").desc())
        self.logger.info('FindTruePositivesTransform >>> pd_time see 5 records')
        pd_time.show(5)
        self.logger.info('FindTruePositivesTransform >>> pd_time Schema')
        pd_time.printSchema()
        df_lookup = (pd_time.filter((pd_time.drive_state == 7) & (pd_time.sig_status == 127))
                     .persist(StorageLevel.MEMORY_AND_DISK))
        
        self.logger.info(f"FindTruePositives check for the count- df_lookup: {df_lookup.count()}")
        self.logger.info(f"FindTruePositives Schema- df_lookup: {df_lookup.printSchema()}")
        self.logger.info(f"FindTruePositives - df_lookup - show 5 records: {df_lookup.show(5)}")

        """ create window fun and get top row if ro_open_dt/rpr_dt and srvc_lbr_ds/cust_conc_cd in df_s360 and df_aws
            are not static"""
 
        # S360 transformation

        # Convert ro_open_dt to TIMESTAMP format
        df_s360 = df_s360.withColumn("ro_open_dt", to_timestamp(col("ro_open_dt"),"yyyy-MM-dd HH:mm:ss"))
        self.logger.info(f'FindTruePositivesTransform >>> df_s360 see 5 records:\n{df_s360.show(5)}')
        self.logger.info(f'FindTruePositivesTransform >>> S360 >> df_s360 Schema:\n {df_s360.printSchema()}')

        df_s360_filtered = df_s360.select("srvc_lbr_ds", "ro_open_dt", "VIN_NB").distinct()

        self.logger.info(f"FindTruePositives check for the count- df_s360_filtered: {df_s360_filtered.count()}")
        self.logger.info(f"FindTruePositives Schema- df_s360_filtered: {df_s360_filtered.printSchema()}")
        self.logger.info(f"FindTruePositives - df_s360_filtered - show 5 records: {df_s360_filtered.show(5)}")

        df_s360_lookup_join = (df_s360_filtered.join(df_lookup, df_lookup["vin"] == df_s360_filtered["VIN_NB"], "inner")
                               .persist(StorageLevel.MEMORY_AND_DISK))
        self.logger.info(f"FindTruePositives check for the count- df_s360_lookup_join: {df_s360_lookup_join.count()}")
        self.logger.info(f"FindTruePositives Schema- df_s360_lookup_join: {df_s360_lookup_join.printSchema()}")
        self.logger.info(f"FindTruePositives - df_s360_lookup_join - show 5 records: {df_s360_lookup_join.show(5)}")

        df_s360_t1 = df_s360_lookup_join.filter(df_s360_lookup_join.timestamp < df_s360_lookup_join.ro_open_dt)
        self.logger.info(f"FindTruePositives check for the count- df_s360_t1: {df_s360_t1.count()}")
        self.logger.info(f"FindTruePositives Schema- df_s360_t1: {df_s360_t1.printSchema()}")
        self.logger.info(f"FindTruePositives - df_s360_t1 - show 5 records: {df_s360_t1.show(5)}")

        df_s360_t1 = df_s360_t1.withColumn("timestamp_rank", f.row_number().over(window))
        df_s360_t1 = df_s360_t1.filter(df_s360_t1.timestamp_rank == 1).drop(df_s360_t1.timestamp_rank)
        df_s360_t1 = (df_s360_t1.withColumn("type", f.lit("S360"))
                      .withColumn("tp_flag", f.when(df_s360_t1.stepin_flg > 0, f.lit(1)).otherwise(f.lit(2)))
                      .withColumnRenamed("timestamp", "latest_data_date")
                      .withColumnRenamed("srvc_lbr_ds", "comments")
                      .withColumnRenamed("ro_open_dt", "rpr_date")
                      .withColumnRenamed("stepin_flg", "flag"))
        df_s360_t1 = df_s360_t1.select('vin', 'rpr_date', 'latest_data_date', 'type', 'flag', 'tp_flag', 'comments')

        self.logger.info(f"FindTruePositives check for the count- df_s360_t1 - final: {df_s360_t1.count()}")
        self.logger.info(f"FindTruePositives Schema- df_s360_t1 - final: {df_s360_t1.printSchema()}")
        self.logger.info(f"FindTruePositives - df_s360_t1 - show 5 records- final: {df_s360_t1.show(5)}")

 
        df_s360_t2 = (df_s360_filtered.filter(
            (df_s360_filtered.ro_open_dt >= min_date) & (df_s360_filtered.ro_open_dt <= max_date)).withColumnRenamed("VIN_NB","vin")
                      .join(df_s360_t1, ["vin"], "leftanti"))
        df_s360_t2 = (df_s360_t2.withColumn("type", f.lit("S360"))
                      .withColumn("tp_flag", f.lit(3))
                      .withColumn("latest_data_date", f.lit(None))
                      .withColumnRenamed("srvc_lbr_ds", "comments")
                      .withColumnRenamed("ro_open_dt", "rpr_date")
                      .withColumn("flag", f.lit(0)))
        df_s360_t2 = df_s360_t2.select('vin', 'rpr_date', 'latest_data_date', 'type', 'flag', 'tp_flag', 'comments')

        self.logger.info(f"FindTruePositives check for the count- df_s360_t2 : {df_s360_t2.count()}")
        self.logger.info(f"FindTruePositives Schema- df_s360_t2 : {df_s360_t2.printSchema()}")
        self.logger.info(f"FindTruePositives - df_s360_t2 - show 5 records: {df_s360_t2.show(5)}")
 
        df_s360_lookup_join.unpersist()
 
        # AWS transformations
        df_aws_filtered = df_aws.select("rpr_dt", "cust_conc_cd", "vin").distinct()

        self.logger.info(f"FindTruePositives check for the count- df_aws_filtered: {df_aws_filtered.count()}")
        self.logger.info(f"FindTruePositives Schema- df_aws_filtered: {df_aws_filtered.printSchema()}")
        self.logger.info(f"FindTruePositives - df_aws_filtered - show 5 records: {df_aws_filtered.show(5)}")

        df_aws_lookup_join = df_aws_filtered.join(df_lookup, ["vin"], "inner").persist(StorageLevel.MEMORY_AND_DISK)

        self.logger.info(f"FindTruePositives check for the count- df_aws_lookup_join: {df_aws_lookup_join.count()}")
        self.logger.info(f"FindTruePositives Schema- df_aws_lookup_join: {df_aws_lookup_join.printSchema()}")
        self.logger.info(f"FindTruePositives - df_aws_lookup_join - show 5 records: {df_aws_lookup_join.show(5)}")
 
        df_aws_t1 = df_aws_lookup_join.filter(df_aws_lookup_join.timestamp < df_aws_lookup_join.rpr_dt)
        df_aws_t1 = df_aws_t1.withColumn("timestamp_rank", f.row_number().over(window))
        df_aws_t1 = df_aws_t1.filter(df_aws_t1.timestamp_rank == 1).drop(df_aws_t1.timestamp_rank)
        df_aws_t1 = (df_aws_t1.withColumn("type", f.lit("AWS"))
                     .withColumn("tp_flag", f.when(df_aws_t1.stepin_flg > 0, f.lit(1)).otherwise(f.lit(2)))
                     .withColumnRenamed("timestamp", "latest_data_date")
                     .withColumnRenamed("cust_conc_cd", "comments")
                     .withColumnRenamed("rpr_dt", "rpr_date")
                     .withColumnRenamed("stepin_flg", "flag"))
        df_aws_t1 = df_aws_t1.select('vin', 'rpr_date', 'latest_data_date', 'type', 'flag', 'tp_flag', 'comments')

        self.logger.info(f"FindTruePositives check for the count- df_aws_t1: {df_aws_t1.count()}")
        self.logger.info(f"FindTruePositives Schema- df_aws_t1: {df_aws_t1.printSchema()}")
        self.logger.info(f"FindTruePositives - df_aws_t1 - show 5 records: {df_aws_t1.show(5)}")
 
        df_aws_t2 = (df_aws_lookup_join.filter(
            (df_aws_lookup_join.rpr_dt >= min_date) & (df_aws_lookup_join.rpr_dt <= max_date))
                     .join(df_aws_t1, ["vin"], "leftanti"))
        df_aws_t2 = (df_aws_t2.withColumn("type", f.lit("AWS"))
                     .withColumn("tp_flag", f.lit(3))
                     .withColumn("latest_data_date", f.lit(None))
                     .withColumnRenamed("cust_conc_cd", "comments")
                     .withColumnRenamed("rpr_dt", "rpr_date")
                     .withColumn("flag", f.lit(0)))
        df_aws_t2 = df_aws_t2.select('vin', 'rpr_date', 'latest_data_date', 'type', 'flag', 'tp_flag', 'comments')

        self.logger.info(f"FindTruePositives check for the count- df_aws_t2: {df_aws_t2.count()}")
        self.logger.info(f"FindTruePositives Schema- df_aws_t2: {df_aws_t2.printSchema()}")
        self.logger.info(f"FindTruePositives - df_aws_t2 - show 5 records: {df_aws_t2.show(5)}")
 
        df_aws_lookup_join.unpersist()
        df_lookup.unpersist()
 
        # Combining all S360 and AWS dataframes
        df_final = df_s360_t1.union(df_s360_t2).union(df_aws_t1).union(df_aws_t2)
        df_final = (df_final.withColumn("rpr_date", f.to_utc_timestamp(col("rpr_date"), "UTC"))
                    .withColumn("latest_data_date", f.to_utc_timestamp(col("latest_data_date"), "UTC"))
                    .withColumn("version", f.lit(0)))
        
        self.logger.info(f"FindTruePositives check for the count- df_final: {df_final.count()}")
        self.logger.info(f"FindTruePositives Schema- df_final: {df_final.printSchema()}")
        self.logger.info(f"FindTruePositives - df_final - show 5 records: {df_final.show(5)}")
 
        return df_final

        
    def find_true_positives(self):
        
        self.logger.info('FindTruePositives >>> Begin')  

        query =[""" select * from `wam_dashboard.aws_rpr` """, """ select * from `wam_dashboard.s360_rpr` """, """ select * from `wam_dashboard.vin_time` """]
        df_aws,df_s360, pd_time = self.bq_query_read(query)
    
        print("Number of rows in df_aws:", df_aws.count(), "| Number of rows in df_s360:", df_s360.count(), "| Number of rows in pd_time:", pd_time.count())

        df_save= self.find_true_positives_transform(df_aws, df_s360,pd_time)

        self.logger.info(f"FindTruePositives check for the count: {df_save.count()}")
        
        self.logger.info('FindTruePositives >>> Writing to BigQuery')

        self.__write_to_bigquery(self.df_save, 'true_pos')
        
        self.logger.info('FindTruePositives >>> End')
        
    def get_cl_projection(self):
        
        self.logger.info('GetCLProjection >>> Starting query')
        query = """ select * from `wam_dashboard.true_pos`"""
        df_p = self.client.query(query).to_dataframe()
        
        self.logger.info('GetCLProjection >>> Get Projection')
        
        df_p = df_p[df_p['tp_flag'] == 1]
        df_p['rpr_date'] = pd.to_datetime(df_p['rpr_date'])
        df_p = df_p.sort_values('rpr_date', ascending=True)

        if df_p.empty:
            self.logger.warning('GetCLProjection >>> No data after sorting by rpr_date')
            return

        end_date =  df_p['rpr_date'].iloc[-1]
        df_p['dummy'] = 1
        df_p['cum_count'] = df_p['tp_flag'].cumsum()
        
        t = (df_p.rpr_date - end_date).dt.total_seconds().values/timedelta(days=1).total_seconds()
        t = t.reshape(-1, 1)
        df_p['days'] = t

        # Continue with the rest of the method
        self.logger.info('GetCLProjection >>> Calculating projection')

        data_new = df_p.groupby(['days'],as_index=False).count()
        data_new['cum_count'] = data_new['dummy'].cumsum(0)
        
        t = data_new.days.values
        t = t.reshape(-1, 1) - 0.5
        count = data_new.cum_count.values
        count = count.reshape(-1,1) 
        
        if len(t) > 1:
            model = linear_model.LinearRegression(fit_intercept=True)
            model.fit(t,count)
            intercept = model.intercept_
            slope = model.coef_
            score = model.score(t, count)

            start_date = pd.to_datetime(end_date)
            value = float((self.cl - intercept) / slope)

            end_date = start_date + timedelta(days=value)


            df_save = pd.DataFrame(list(zip([start_date,end_date],\
                                                [0,self.cl],
                                                [str(intercept),str(intercept)],\
                                                [str(slope),str(slope)],\
                                                [str(score),str(score)],\

                                               )),
                                       columns =['pr_date','pr_val','pr_int','pr_sl','pr_sc'])

            self.logger.info('GetCLProjection >>> Writing to BigQuery')

            job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("pr_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
                        bigquery.SchemaField("pr_val", bigquery.enums.SqlTypeNames.INTEGER),
                        bigquery.SchemaField("pr_int", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("pr_sl", bigquery.enums.SqlTypeNames.STRING),
                        bigquery.SchemaField("pr_sc", bigquery.enums.SqlTypeNames.STRING),
                    ],
                    write_disposition="WRITE_TRUNCATE",)

            job = self.client.load_table_from_dataframe(df_save, 'wam_dashboard.projection', job_config=job_config)
            job.result()

            self.logger.info('GetCLProjection >>> End')
            
        else:
            self.logger.info('GetCLProjection >>> Ended without calculating projection, not enough true positives (>1)...')
