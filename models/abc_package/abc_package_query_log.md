{% docs abc_package_query_log %}

# abc_package_query_log

This model retrieves information about Google BigQuery jobs in the US region by querying the `INFORMATION_SCHEMA.JOBS_BY_PROJECT` table. The selected columns include the job's project ID, user email, job ID, job type, start time, end time, query, and any error details if applicable. The results are ordered by start time in descending order. This model can be used to analyze BigQuery job performance and troubleshoot any errors that may occur during job execution.

- `project_id`: The ID of the project where the job was run.
- `user_email`: The email of the user who ran the job.
- `job_id`: The ID of the job.
- `job_type`: The type of the job (query, load, etc.).
- `start_time`: The time when the job started.
- `end_time`: The time when the job ended.
- `query`: The SQL query that was run (if the job type is "query").
- `error_result.reason`: The reason for any errors that occurred during the job.
- `error_result.location`: The location of any errors that occurred during the job.
- `error_result.message`: The error message for any errors that occurred during the job.

The results are ordered by start time in descending order. 

{% enddocs %}