# vendor

v1_jobs_1h file contains jobs updates from Automation vendor. Task is to do the following

- Load and parse the job updates

- Filter the updates for farm 3

- Find difference between timestamps (timestamp and UpdatedAt field). Note - timestamp is a top-level field where as UpdatedAt is present within each job update JSON

- Job updates can have multiple updates for the same job across multiple log lines. Sort them updates for a given job id (identified by “Id” field) and pick only the first update

- Objective: Find the minimum time difference (latency) between the system getting to know a job update from automation vendor for each job.

- Write the min latency for each job id to an output file
