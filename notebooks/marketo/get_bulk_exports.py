# Databricks notebook source

import logging, time
import pendulum

from data_sources.marketo.classes import MarketoREST
from data_sources.marketo.reference.lead_fields import lead_fields

# COMMAND ----------
job_variables = {
    "leads": {
        "fields": {"fields": lead_fields},  # there's a good reason for this
        "export_filter": "updatedAt",
    },
    "activities": {
        "fields": {},  # this too
        "export_filter": "createdAt",
    },
}
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
w_api_object = dbutils.widgets.get("api_object")
w_start_date = dbutils.widgets.get("start_date")
w_days = dbutils.widgets.get("data_window_in_days")
w_job_id = dbutils.widgets.get("job_id")

object_variables = job_variables[w_api_object]
start_date = pendulum.parse(w_start_date)
end_date = start_date.add(days=int(w_days))

marketo = MarketoREST()
# COMMAND ----------
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
    if job_status in ["Queued", "Processing"]:
        time.sleep(15)
        continue
    if job_status in ["Completed"]:
        break

# COMMAND ----------
filepath = marketo.load_completed_job_to_raw(api_object=w_api_object, job_id=job_id)
print(filepath)

# COMMAND ----------
if not marketo.verify_export_file_integrity(filepath, checksum):
    raise ValueError("File in storage did not API checksum")
