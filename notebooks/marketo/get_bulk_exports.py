# Databricks notebook source
# DBTITLE 1, Imports and Variables
import logging, time
import pendulum

from data_sources.marketo.classes import MarketoREST


marketo = MarketoREST()
job_variables = {
    "leads": {
        # This object has ~1300 fields which have to be listed explicitly
        "fields": {"fields": marketo.get_lead_fields()},
        "export_filter": "updatedAt",
    },
    "activities": {
        # This object automatically retrieves all fields if param is left empty
        "fields": {},
        "export_filter": "createdAt",
    },
}
# COMMAND ----------
# DBTITLE 1, Widgets for Manual Runs
dbutils.widgets.dropdown(
    name="api_object",
    defaultValue=list(job_variables.keys())[0],
    choices=job_variables.keys(),
    label="1. API object",
)
dbutils.widgets.text(
    name="start_date",
    defaultValue=str(pendulum.now().date().subtract(days=1)),
    label="2. Start date (YYYY-MM-DD)",
)
dbutils.widgets.text(
    name="data_window_in_days", defaultValue="1", label="3. Data window in days"
)
dbutils.widgets.text(name="job_id", defaultValue="", label="4. Manual job id")

# COMMAND ----------
# DBTITLE 1, Setup for Extract
w_api_object = dbutils.widgets.get("api_object")
w_start_date = dbutils.widgets.get("start_date")
w_days = dbutils.widgets.get("data_window_in_days")
w_job_id = dbutils.widgets.get("job_id")

object_variables = job_variables[w_api_object]
start_date = pendulum.parse(w_start_date)
end_date = start_date.add(days=int(w_days))

# COMMAND ----------
# DBTITLE 1, Create or Retreive Job ID
if w_job_id == "":
    job_id = marketo.create_async_export_job(
        api_object=w_api_object,
        start_date=start_date,
        end_date=end_date,
        export_filter=object_variables["export_filter"],
        fields=object_variables["fields"],
    )
    print(f"Job ID:\t{job_id}")
    marketo.enqueue_async_export(api_object=w_api_object, job_id=job_id)
else:
    job_id = w_job_id
    print(f"Manual job ID:\t{job_id}")

# COMMAND ----------
# DBTITLE 1, Await Job Completion
while True:
    job_status, checksum = marketo.check_async_export_status(
        api_object=w_api_object, job_id=job_id
    )
    print(f'Job status - "{job_status}"\t{pendulum.now()}')

    if job_status in ["Created", "Canceled", "Failed"]:
        logging.error("Remote API async job failed or was never started")
        raise ValueError(
            f'Job status is "{job_status}". Expected "Queued" or "Processing"'
        )
    elif job_status in ["Queued", "Processing"]:
        time.sleep(15)
        continue
    elif job_status in ["Completed"]:
        break
    else:
        raise ValueError("Unexpected job status returned.")

# COMMAND ----------
# DBTITLE 1, Load File to Raw
filepath = marketo.load_completed_job_to_raw(api_object=w_api_object, job_id=job_id)
print(filepath)

# COMMAND ----------
# DBTITLE 1, Compare File to Checksum
if not marketo.verify_export_file_integrity(filepath, checksum):
    logging.warning(filepath)
    raise ValueError("File in storage did not match API checksum")
