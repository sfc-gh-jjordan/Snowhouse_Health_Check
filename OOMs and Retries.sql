use role SALES_ENGINEER;
use warehouse SNOWADHOC;
use database SNOWHOUSE_IMPORT;

--use schema AZEASTUS2PROD;
--use schema azwesteurope;
use schema gcpuscentral1;

select name as account_locator, alias as account_name
from account_etl_v
where name = 'DATA';

set account_id = 68082;

set days_back = 31;

--OOM counts by day
select
    user_name,
    to_date(created_on) as created_date,
    count(*) as count_jobs
FROM job_etl_v 
WHERE account_id = $account_id 
and created_on >= dateadd(dd,-$days_back,current_date())
and stats:stats:oomKillCount is not null
group by 1,2
order by 3 desc,1;


--OOMs Detail
 select
    uuid as query_id
    , user_name
    , warehouse_name
    , case 
        when stats:warehouseSize = 1 then 'X-Small'
        when stats:warehouseSize = 2 then 'Small'
        when stats:warehouseSize = 4 then 'Medium'
        when stats:warehouseSize = 8 then 'Large'
        when stats:warehouseSize = 16 then 'X-Large' 
        when stats:warehouseSize = 32 then '2X-Large'
        when stats:warehouseSize = 64 then '3X-Large'
        when stats:warehouseSize = 128 then '4X-Large'
    end as wh_size
    , stats:stats:retryCount as retry_count
    , stats:stats:oomKillCount as oom_count
    --, dur_queued_load as queued_ms
    --, (dur_queued_load/60000.00) as queued_minutes
    --, ROUND(((dur_queued_load/total_duration) * 100),2) as queued_percentage
    , total_duration as duration_ms
    , (total_duration/1000.00) as duration_sec
    , (total_duration/60000.00) as duration_min
    --, dur_xp_executing
    , (dur_xp_executing/1000/60) as exec_minutes
    , database_name
    , created_on as created_dt_utc
    --, TO_DATE(DATEADD(hh,-6,created_on)) as created_date_cst --update to whatever time zone
    --, TO_TIME(DATEADD(hh,-6,created_on)) as created_time_cst --update to whatever time zone
    , latest_cluster_number
    --, tag as query_tag
    , error_code
    , error_message
    , stats:stats:ioLocalFdnReadBytes as ioLocalFdnReadBytes
    , stats:stats:ioRemoteFdnReadBytes as ioRemoteFdnReadBytes
    , stats:stats:ioLocalFdnWriteBytes as ioLocalFdnWriteBytes
    , stats:stats:ioRemoteFdnWriteBytes as ioRemoteFdnWriteBytes
    , substr(description,1,250) as sql_text_partial --if SQL text large get subset
FROM job_etl_v 
WHERE
   account_id = $account_id 
   and created_on >= dateadd(dd,-$days_back,current_date())
   --and created_on >= '2025-06-15' and created_on < '2025-07-16'
   and stats:stats:oomKillCount is not null --OOMs only
   --and warehouse_name in('<warehouse_name>')
   --and uuid in('01ac3c82-0b04-8206-0000-280169e6e28f','01ac5457-0b04-874e-0000-28016c14faef','01ac4f2f-0b04-85e1-0000-28016b674d37')
  /* AND (
        // Job has been restarted with a new job
        restart_job_id > 0
        // Job was a restart from previous job
        or restarted_from_job_id is not null
        // Number of XP Retries
        or stats:stats:retryCount is not null
        // Number of XP OOMs that was reason for XP retry
        or stats:stats:oomKillCount is not null)*/
ORDER BY duration_min desc;


--Incident Failures by Day - Summary
select
    user_name,
    to_date(created_on) as created_date,
    count(*) as count_jobs
FROM job_etl_v 
WHERE account_id = $account_id 
and created_on >= dateadd(dd,-$days_back,current_date())
and error_code = '000603' --Failure with incident
group by 1,2
order by 3 desc,1;

--Incident Failures by Day - Details
select
    uuid as query_id
    , user_name
    , warehouse_name
    , case 
        when stats:warehouseSize = 1 then 'X-Small'
        when stats:warehouseSize = 2 then 'Small'
        when stats:warehouseSize = 4 then 'Medium'
        when stats:warehouseSize = 8 then 'Large'
        when stats:warehouseSize = 16 then 'X-Large' 
        when stats:warehouseSize = 32 then '2X-Large'
        when stats:warehouseSize = 64 then '3X-Large'
        when stats:warehouseSize = 128 then '4X-Large'
    end as wh_size
    , stats:stats:retryCount as retry_count
    , stats:stats:oomKillCount as oom_count
    --, dur_queued_load as queued_ms
    --, (dur_queued_load/60000.00) as queued_minutes
    --, ROUND(((dur_queued_load/total_duration) * 100),2) as queued_percentage
    , total_duration as duration_ms
    , (total_duration/1000.00) as duration_sec
    , (total_duration/60000.00) as duration_min
    --, dur_xp_executing
    , (dur_xp_executing/1000/60) as exec_minutes
    , database_name
    , created_on as created_dt_utc
    --, TO_DATE(DATEADD(hh,-6,created_on)) as created_date_cst --update to whatever time zone
    --, TO_TIME(DATEADD(hh,-6,created_on)) as created_time_cst --update to whatever time zone
    , latest_cluster_number
    --, tag as query_tag
    , error_code
    , error_message
    , stats:stats:ioLocalFdnReadBytes as ioLocalFdnReadBytes
    , stats:stats:ioRemoteFdnReadBytes as ioRemoteFdnReadBytes
    , stats:stats:ioLocalFdnWriteBytes as ioLocalFdnWriteBytes
    , stats:stats:ioRemoteFdnWriteBytes as ioRemoteFdnWriteBytes
    , substr(description,1,250) as sql_text_partial --if SQL text large get subset
FROM job_etl_v 
WHERE account_id = $account_id 
and created_on >= dateadd(dd,-$days_back,current_date())
and error_code = '000603' --Failure with incident
order by created_on desc;

