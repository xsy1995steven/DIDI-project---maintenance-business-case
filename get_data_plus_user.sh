hive -e "
DROP TABLE IF EXISTS am_temp.xsc_mta_pre_data_xie;
set mapreduce.reduce.memory.mb=10240;
ADD JAR hdfs:///user/supply_chain/program/jar/geohash-function.jar;
CREATE TEMPORARY FUNCTION GEOHASH_DECODE AS 'com.xiaoju.automarket.GeohashDecode';
CREATE TEMPORARY FUNCTION GEOHASH_ENCODE AS 'com.xiaoju.automarket.GeohashEncode';
set mapred.job.queue.name=root.kuaicheshiyebu-houshichangyewuxian.amdwdev; 
CREATE TABLE am_temp.xsc_mta_pre_data_xie AS
select   
a.geohash_7 as geohash_7
,a.geohash_6 as  geohash_6
,a.geohash_5 as  geohash_5        
,a.uid as uid
,a.hh as hh
,a.date1 as date1
,stay_time_cut
,a.mta_haixiu_store_geohash_6 as mta_haixiu_store_geohash_6
,geohash_6_nums
,geohash_5_nums
,geohash_6_hours
,geohash_6_avg_hour
,geohash_6_max_hour
,geohash_6_min_hour
,geohash_5_hours
,geohash_5_avg_hour
,geohash_5_max_hour
,geohash_5_min_hour
,is_ordered
,used_last_two_weeks
,used_last_month
,used_last_two_month
,row_number() over (partition by a.uid order by a.hh desc) as ranks
,row_number() over (partition by a.uid,a.geohash_6,a.date1 order by geohash_6_hours) as rank_geo_6
,row_number() over (partition by a.uid,a.geohash_5,a.date1 order by geohash_5_hours) as rank_geo_5
,driver_type
,driver_join_model
,driver_verify_status
,is_auth_driver,order_finish_count_1d
,order_finish_count_1w
,order_finish_count_1m
,order_finish_distance_1d
,order_finish_distance_1w
,order_finish_distance_1m
,morning_peak_order_finish_distance_1d
,morning_peak_order_finish_distance_1w
,morning_peak_order_finish_distance_1m
,night_peak_order_finish_distance_1d
,night_peak_order_finish_distance_1w
,night_peak_order_finish_distance_1m
,num_store
from 
(
  select  
    uid
    ,substr(geohash,1,7) as geohash_7
    ,substr(geohash,1,6) as geohash_6
    ,substr(geohash,1,5) as geohash_5
    ,hh
    ,sum(stay_time_cut) as stay_time_cut
    ,mta_haixiu_store_geohash_6
    ,date1
    ,is_ordered
  from 
    am_dw.mta_haixiu_pid_driving_info_other_result_1_daily
  where 
    dt>='20180715' and  area=4
  group by 
    uid
    ,substr(geohash,1,7)
    ,substr(geohash,1,6)
    ,substr(geohash,1,5)
    ,hh
    ,mta_haixiu_store_geohash_6
    ,date1
    ,is_ordered
)a
left join
(
  select 
    count(distinct substr(geohash,1,6)) geohash_6_nums
    ,count(distinct substr(geohash,1,5)) geohash_5_nums
    ,uid
    ,hh
    ,date1
  from 
    am_dw.mta_haixiu_pid_driving_info_other_result_1_daily
  where 
    dt>='20180715' and  area=4
  group by 
    uid,hh,date1
)b on a.uid=b.uid and a.hh=b.hh and a.date1=b.date1
left join
(
  select  
    substr(geohash,1,6) as geohash_6
    ,uid
    ,date1
    ,count(distinct hh) geohash_6_hours
    ,avg(int(substr(hh,12,2))) geohash_6_avg_hour
    ,max(int(substr(hh,12,2))) geohash_6_max_hour
    ,min(int(substr(hh,12,2))) geohash_6_min_hour
  from 
    am_dw.mta_haixiu_pid_driving_info_other_result_1_daily
  where 
    dt>='20180715' and area=4
  group by 
    uid,substr(geohash,1,6),date1
)c on a.uid=c.uid  and a.date1=c.date1 and a.geohash_6=c.geohash_6
left join
(
  select  
    substr(geohash,1,5) as geohash_5
    ,uid
    ,date1
    ,count(distinct hh) geohash_5_hours
    ,avg(int(substr(hh,12,2))) geohash_5_avg_hour
    ,max(int(substr(hh,12,2))) geohash_5_max_hour
    ,min(int(substr(hh,12,2))) geohash_5_min_hour
  from 
    am_dw.mta_haixiu_pid_driving_info_other_result_1_daily
  where 
    dt>='20180715' and area=4
  group by 
    uid,substr(geohash,1,5),date1
)d on a.uid=d.uid  and a.date1=d.date1 and a.geohash_5=d.geohash_5
left join
(
  select 
    ee.driver_id as driver_id,count(ee.create_time) as used_last_two_weeks
  from 
  (
    select 
      distinct driver_id, create_time
    from 
      am_temp.mta_haixiu_pid_order_time
    where 
      create_date > '2018-07-17'  
  )ee 
  group by 
    ee.driver_id
)e on a.uid = e.driver_id
left join
(
  select 
    ff.driver_id as driver_id,count(ff.create_time) as used_last_month
  from 
  (
    select 
      distinct driver_id, create_time
    from 
      am_temp.mta_haixiu_pid_order_time
    where 
      create_date > '2018-06-21'
  )ff
  group by
      ff.driver_id
)f on a.uid = f.driver_id
left join
(
  select 
    gg.driver_id as driver_id,count(gg.create_time) as used_last_two_month
  from 
  (
    select 
      distinct driver_id, create_time
    from 
      am_temp.mta_haixiu_pid_order_time
    where 
      create_date > '2018-05-17'
  )gg
  group by 
    gg.driver_id
)g on a.uid = g.driver_id
left join 
(
  select 
    driver_id,driver_type,driver_join_model,driver_verify_status,
    is_auth_driver,order_finish_count_1d,order_finish_count_1w,
    order_finish_count_1m,order_finish_distance_1d,order_finish_distance_1w,
    order_finish_distance_1m,morning_peak_order_finish_distance_1d,
    morning_peak_order_finish_distance_1w,morning_peak_order_finish_distance_1m,
    night_peak_order_finish_distance_1d,night_peak_order_finish_distance_1w,
    night_peak_order_finish_distance_1m
  from 
    am_dw.csum_driver_integrate_info
  where  
    dt = '20180805'
)i on a.uid = i.driver_id
left join
(
  select
    count(distinct x.name) as num_store,x.geohash_7 as geohash_7
  from 
    (
      select 
        name,GEOHASH_ENCODE(double(coalesce(lon,0)), double(coalesce(lat,0)), 7) as geohash_7
      from 
        am_dw.dwd_mta_store_info_extend
      where dt='20180727'
    )x
  group by 
    x.geohash_7
)j on a.geohash_7 = j.geohash_7
inner join
(
  select 
    GEOHASH_ENCODE(double(coalesce(lon,0)), double(coalesce(lat,0)), 6) as mta_haixiu_store_geohash_6
  from 
    am_dw.dim_mta_store_info
  where 
    dt='20180805' and lat is not null
        AND status=1 AND biz_status=1
        and name like '%嗨修%' 
  group by 
    GEOHASH_ENCODE(double(coalesce(lon,0)), double(coalesce(lat,0)), 6)
)h on a.geohash_6=h.mta_haixiu_store_geohash_6;
">xsc_mta_pre_data_xie.csv 