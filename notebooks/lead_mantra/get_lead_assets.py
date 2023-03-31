# Databricks notebook source

import asyncio, json, math
from pathlib import Path

import numpy as np

from data_sources.lead_mantra.classes import LeadMantraREST
from shared.functions.file_io import generate_unique_filename
from shared.functions.azure_utilities import get_mount_paths

# COMMAND ----------
dbutils.widgets.dropdown(
    "asset",
    "message_log",
    ["message_log", "lead_status", "message_attempt"],
    "1. Asset",
)
dbutils.widgets.dropdown(
    "lead_status", "both", ["open", "closed", "both"], "2. Where lead status is..."
)

# COMMAND ----------
lm = LeadMantraREST()
lm.get_campaigns()

w_asset = dbutils.widgets.get("asset")
w_lead_status = dbutils.widgets.get("lead_status")

dir_path = f"{lm.storage_paths.landing}/{w_asset}"
Path(f"/dbfs/{dir_path}").mkdir(exist_ok=True)

filepath = f"{dir_path}/{generate_unique_filename(w_asset)}.jsonl"
print("filepath:\n", filepath)
# COMMAND ----------
bronze_root = get_mount_paths("lead_mantra").bronze
bronze_leads = spark.read.load(path=f"{bronze_root}/leads")

filter_status = {"open": "1", "closed": "0", "both": "0,1"}
id_pairs = (
    bronze_leads.filter(f"lead_status in ({filter_status[w_lead_status]})")
    .filter("external_system_id != ''")
    .select(["campaign_id", "external_system_id"])
    .distinct()
    .collect()
)
chunked_id_pairs = np.array_split(id_pairs, math.ceil(len(id_pairs) / 5000))
# COMMAND ----------
# TODO - move this somewhere more reasonable...
json_key_map = {
    "message_log": "message_logs",
    "lead_status": "lead_details",
    "message_attempt": "lead_details",
}
# COMMAND ----------
with open(f"/dbfs/{filepath}", "a") as file:
    for n, sublist in enumerate(chunked_id_pairs):
        print(f"Running batch {n+1} of {len(chunked_id_pairs)}...")
        data = asyncio.run(
            lm.get_lead_asset(
                endpoint=f"campaign-leads/get_{w_asset}",
                json_key=json_key_map[w_asset],
                id_pairs=sublist,
            )
        )
        file.writelines((json.dumps(row) + "\n" for row in data))
        print(f"Batch {n+1} complete - {len(data)} rows written to file.")
# COMMAND ----------
df = spark.read.json(filepath)
display(df)
