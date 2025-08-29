use role SALES_ENGINEER;
use warehouse SNOWADHOC;
use database SNOWHOUSE_IMPORT;

--describe table job_etl_v;

--use schema azeastus2prod;
--set account_id = 3693;

use schema gcpuscentral1;
set account_id = 68082;

set days_back = 31;

set threshold_grain = 1000; --for seconds

--Action Item:  If large % of queries completing within acceptable SLA times can these be downsized or route/segment these to another WH?
--For Front End workloads should I enable QAS for outlier queries?
--For Front End workloads look at low usage periods by hour/day where you can downsize
--For Front End workloads, if not much room for error or controlling when users run things then adjust auto-suspend from 10 minutes to 5 minutes for low usage periods
--For Back End workloads always set auto-suspend time to 1 minute or explicity suspend warehouse after ETL job


--For given warehouse and time period identify % of queries within each bucket
--Determine if a high percentage of queries could use a smaller warehouse
--If feasbile segment these to another warehouse
select 
    warehouse_name
    , to_date(created_on) as start_date
    , date_trunc(HOUR,created_on) as start_hour
    , DAYOFWEEK(created_on) as day_of_week
    --, min(created_on) as start_period
    --, max(created_on) as end_period
    , count(*) query_count
    , sum(case when error_code is not null then 1 else 0 end) as error_count
    , sum(case when error_code = '000603' then 1 else 0 end) as incident_error_count
    , avg(total_duration/$threshold_grain) as avg_duration_seconds
    , max(total_duration/$threshold_grain) as max_duration_seconds
    , avg(dur_queued_load/$threshold_grain) as avg_queued_seconds
    , max(dur_queued_load/$threshold_grain) as max_queued_seconds
    , sum(stats:stats:ioLocalFdnReadBytes + stats:stats:ioRemoteFdnReadBytes) as bytes_scanned
    , avg(stats:stats:ioLocalFdnReadBytes + stats:stats:ioRemoteFdnReadBytes) as avg_bytes_scanned
    , sum(case when stats:stats.ioRemoteFdnReadBytes > (1024*1024*1000) then 1 else 0 end) as remote_spill_GB
    --, sum(case when error_code is not null then 1 else 0 end) as error_count
    , sum(case when total_duration < ($threshold_grain*10) then 1 else 0 end) as dur_under_10
    , sum(case when total_duration/$threshold_grain < 10 then 1 else 0 end)/count(*) as pct_under_10
    , sum(case when total_duration < ($threshold_grain*30) then 1 else 0 end) as dur_under_30
    , sum(case when total_duration/$threshold_grain < 30 then 1 else 0 end)/count(*) as pct_under_30
    , sum(case when total_duration < ($threshold_grain*60) then 1 else 0 end) as dur_under_60
    , sum(case when total_duration/$threshold_grain < 60 then 1 else 0 end)/count(*) as pct_under_60
    , sum(case when total_duration < ($threshold_grain*120) then 1 else 0 end) as duration_under_120
    , sum(case when total_duration/$threshold_grain < 120 then 1 else 0 end)/count(*) as pct_under_120
FROM job_etl_v 
WHERE
   account_id = $account_id 
   --and dur_queued_load > 1000*60*15
   --and stats:stats.ioRemoteFdnReadBytes > (1024*1024*1024*1000) --with 252 scanning over 1TB remotely and 101 scanning over 2TB remotely vs. 145 and 71
   --and created_on >= dateadd(dd,-$days_back,current_date())
   and created_on >= '2025-06-15' and created_on < '2025-07-15'
   and warehouse_name in('TRANSFORMING_WH') --in('SUPPLYCHAIN_WH1') --in('TRANSFORMING_WH') --in('HONEYDEW_WH')  --in('PROD_C360_WH')   --in('HONEYDEW_WH')
group by all
order by 1,2,3; 


/*******************************************************/
set threshold_grain = 60000; --for minutes

--For given warehouse and time period identify % of queries within each bucket
--Determine if a high percentage of queries could use a smaller warehouse
--If feasbile segment these to another warehouse
select 
    warehouse_name
    , to_date(created_on) as start_date
    , date_trunc(HOUR,created_on) as start_hour
    , DAYOFWEEK(created_on) as day_of_week
    --, min(created_on) as start_period
    --, max(created_on) as end_period
    , count(*) query_count
    , sum(case when error_code is not null then 1 else 0 end) as error_count
    , sum(case when error_code = '000603' then 1 else 0 end) as incident_error_count
    , avg(total_duration/$threshold_grain) as avg_duration_minutes
    , max(total_duration/$threshold_grain) as max_duration_minutes
    , avg(dur_queued_load/$threshold_grain) as avg_queued_minutes
    , max(dur_queued_load/$threshold_grain) as max_queued_minutes
    , sum(stats:stats:ioLocalFdnReadBytes + stats:stats:ioRemoteFdnReadBytes) as bytes_scanned
    , avg(stats:stats:ioLocalFdnReadBytes + stats:stats:ioRemoteFdnReadBytes) as avg_bytes_scanned
    , sum(case when stats:stats.ioRemoteFdnReadBytes > (1024*1024*1000) then 1 else 0 end) as remote_spill_GB
    --, sum(case when error_code is not null then 1 else 0 end) as error_count
    , sum(case when total_duration < ($threshold_grain*1) then 1 else 0 end) as dur_under_1
    , sum(case when total_duration/$threshold_grain < 1 then 1 else 0 end)/count(*) as pct_under_1
    , sum(case when total_duration < ($threshold_grain*5) then 1 else 0 end) as dur_under_5
    , sum(case when total_duration/$threshold_grain < 5 then 1 else 0 end)/count(*) as pct_under_5
    , sum(case when total_duration < ($threshold_grain*10) then 1 else 0 end) as dur_under_10
    , sum(case when total_duration/$threshold_grain < 10 then 1 else 0 end)/count(*) as pct_under_10
    , sum(case when total_duration < ($threshold_grain*30) then 1 else 0 end) as dur_under_30
    , sum(case when total_duration/$threshold_grain < 30 then 1 else 0 end)/count(*) as pct_under_30
    , sum(case when total_duration < ($threshold_grain*60) then 1 else 0 end) as duration_under_60
    , sum(case when total_duration/$threshold_grain < 60 then 1 else 0 end)/count(*) as pct_under_60
    , sum(case when total_duration < ($threshold_grain*120) then 1 else 0 end) as duration_under_120
    , sum(case when total_duration/$threshold_grain < 120 then 1 else 0 end)/count(*) as pct_under_120
FROM job_etl_v 
WHERE
   account_id = $account_id 
   and created_on >= '2025-06-15' and created_on < '2025-07-15'
   --and dur_queued_load > 1000*60*15
   --and stats:stats.ioRemoteFdnReadBytes > (1024*1024*1024*1000) --with 252 scanning over 1TB remotely and 101 scanning over 2TB remotely vs. 145 and 71
   --and created_on >= dateadd(dd,-$days_back,current_date())
   and warehouse_name in('SUPPLYCHAIN_WH1') --in('SUPPLYCHAIN_TASK_WH1')  --in('PROD_C360_WH')   --in('HONEYDEW_WH')
group by all
order by 1,2,3;






