{{ config(tags=['gbq']) }}

select

    project_id            as gbq_job_project_id, 
    user_email            as gbq_job_user_email, 
    job_id                as gbq_job_id, 
    job_type              as gbq_job_type, 
    start_time            as gbq_job_start_time,
    end_time              as gbq_job_end_time,
    query                 as gbq_job_query, 
    error_result.reason   as gbq_job_reason, 
    error_result.location as gbq_job_location, 
    error_result.message  as gbq_job_message
    
from region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
order by start_time desc