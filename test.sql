

SELECT 
  vin,
  df_partition_date as date, 
  veh_model, 
  model_year as year, 
  monitor_timestamp as timestamp,
  hit_count_1 as hitcount1,
  hit_count_2 as hitcount2, 
  step_in_flag as stepin_flg,
  
FROM 
    `{}.bq_34_tcu4g_feature_fdp_dwc_vw.ncvdc62_fnv2_psa_vw`

