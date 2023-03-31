# Databricks notebook source

import pendulum
import asyncio
import json
from pathlib import Path

from data_sources.lead_mantra.classes import LeadMantraREST
from shared.functions.file_io import generate_unique_filename

# COMMAND ----------
dbutils.widgets.text(
    "start_date", str(pendulum.now().date().subtract(days=3)), "1. Start Date"
)
# COMMAND ----------
lm = LeadMantraREST()
lm.get_campaigns()

w_start_date = dbutils.widgets.get("start_date")

dir_path = f"{lm.storage_paths.landing}/leads"
Path(f"/dbfs/{dir_path}").mkdir(exist_ok=True)

filepath = f"{dir_path}/{generate_unique_filename('leads')}.jsonl"
print("filepath:\n", filepath)
# COMMAND ----------
with open(f"/dbfs/{filepath}", "a") as file:
    for cid in lm.campaign_ids:
        print(f"Starting campaign [{cid}]...")
        leads = asyncio.run(
            lm.get_leads(
                campaign_id=cid,
                start_date=w_start_date,
            ),
        )
        file.writelines((json.dumps(row) + "\n" for row in leads))
        print(f"Campaign [{cid}] complete - {len(leads)} rows written to file.")
# COMMAND ----------
df = spark.read.json(filepath)
display(df)
