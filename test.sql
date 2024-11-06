
DECLARE const int64 default 430;
DECLARE leaky_slope float64 default -1.5;
DECLARE normal_slope float64 default 0;
DECLARE residual_standard_deviation float64 default 0.004;

DECLARE end_time timestamp DEFAULT CURRENT_TIMESTAMP;
DECLARE start_time timestamp DEFAULT TIMESTAMP_SUB(end_time, INTERVAL 30 DAY);

create temp function mock_tire(const int64, slope float64,st float64,time_s float64) 
returns float64
as 
(
rand()*st+const+slope*time_s
);


create or replace temp table mock_type_data as (

   with range_query 
  as 
  (
    SELECT id FROM UNNEST(GENERATE_ARRAY(UNIX_SECONDS(start_time),UNIX_SECONDS(end_time),3600)) as id
  ),

  -- leaky_tire
   leaky_tire as (
  select 
  TIMESTAMP_SECONDS(id) as time_s,
  mock_tire(const,leaky_slope,residual_standard_deviation,time_s) as t_press
  from 
  (
    select
    id,
    (id - UNIX_SECONDS(end_time))/(3600*24) as time_s
    from range_query
  )
  ),


  -- normal_tire
   normal_tire as 
  (
  select 
  TIMESTAMP_SECONDS(id) as time_s,
  mock_tire(const,normal_slope,residual_standard_deviation,time_s) as t_press
  from 
  (
    select
    id,
    (id - UNIX_SECONDS(end_time))/(3600*24) as time_s
    from range_query
  )
  ),

  --one_sample_tire
   one_sample_tire as
  (
    select * from normal_tire limit 1
  )


  -- Case for Vehicle with 1 leak
  select 
  time_s, t_press,
  'LF' as tire_id,
  'FAKE001' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from leaky_tire

  UNION ALL


  select 
  time_s, t_press,
  'RF' as tire_id,
  'FAKE001' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire


  UNION ALL


  select 
  time_s, t_press,
  'ORR' as tire_id,
  'FAKE001' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  UNION ALL


  select 
  time_s, t_press,
  'ORL' as tire_id,
  'FAKE001' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  --Case for Vehicle with 2 leaks
  UNION ALL
  select 
  time_s, t_press,
  'LF' as tire_id,
  'FAKE002' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from leaky_tire

  UNION ALL


  select 
  time_s, t_press,
  'RF' as tire_id,
  'FAKE002' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire


  UNION ALL


  select 
  time_s, t_press,
  'ORR' as tire_id,
  'FAKE002' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from leaky_tire

  UNION ALL


  select 
  time_s, t_press,
  'ORL' as tire_id,
  'FAKE002' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  --Case for Vehicle with no leaks

  UNION ALL
  select 
  time_s, t_press,
  'LF' as tire_id,
  'FAKE003' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  UNION ALL


  select 
  time_s, t_press,
  'RF' as tire_id,
  'FAKE003' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire


  UNION ALL


  select 
  time_s, t_press,
  'ORR' as tire_id,
  'FAKE003' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  UNION ALL


  select 
  time_s, t_press,
  'ORL' as tire_id,
  'FAKE003' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  -- #Case for Vehicle with tire with 1 datapoint

  UNION ALL
  select 
  time_s, t_press,
  'LF' as tire_id,
  'FAKE004' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from one_sample_tire

  UNION ALL


  select 
  time_s, t_press,
  'RF' as tire_id,
  'FAKE004' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire


  UNION ALL


  select 
  time_s, t_press,
  'ORR' as tire_id,
  'FAKE004' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  UNION ALL


  select 
  time_s, t_press,
  'ORL' as tire_id,
  'FAKE004' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire

  -- #Case for Vehicle with 1 tire

  UNION ALL

  select 
  time_s, t_press,
  'LF' as tire_id,
  'FAKE005' as vin,
  cast(time_s as date) as partition_date,
  10000 as odo,720 as vin_tire_qty,
  5.0 as ftcp_version,
  16 as amb_temp
  from normal_tire
);


insert into `tsl_na_prognostics.mock_tsl_preprocesed`
(
  time_s,t_press,tire_id,vin,partition_date,vin_tire_qty,ftcp_version,amb_temp
)
select time_s,t_press,tire_id,vin,partition_date,vin_tire_qty,ftcp_version,amb_temp from mock_type_data;

-- tsl_messaging
create or replace temp table tsl_messaging_test AS
(
  -- #Case for tire with previous not cleared leak that was still detected as leaky
  select 
  'FAKE002' as vin,
  'LF' as tyre,
  current_datetime() as detection_time,
  null as clear_time,
  null as clear_condition

  UNION ALL

  -- #Case for tire with cleared leak that was detected as leaky

  select 
  'FAKE002' as vin,
  'ORR' as tyre,
  current_datetime() as detection_time,
  current_datetime() as clear_time,
  'training_unconfirmed|batch' as clear_condition

  UNION ALL

  -- #Case for tire with previous not cleared leak that was detected as not leaky

  select 
  'FAKE002' as vin,
  'RF' as tyre,
  current_datetime() as detection_time,
  null as clear_time,
  null as clear_condition

  UNION ALL

  -- #Case for tire with cleared leak that was still detected as not leaky

  select 
  'FAKE002' as vin,
  'ORL' as tyre,
  current_datetime() as detection_time,
  current_datetime() as clear_time,
  'training_unconfirmed' as clear_condition

  
);

delete FROM `tsl_na_prognostics.tsl_messaging_test` where vin is not null;

insert into `tsl_na_prognostics.tsl_messaging_test`
(vin,tyre,detection_time,clear_time,clear_condition)
select vin,tyre,detection_time,clear_time,clear_condition from  tsl_messaging_test



