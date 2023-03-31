# Databricks notebook source
from pathlib import Path

import json

from data_sources.lead_mantra.classes import LeadMantraREST
from shared.functions.file_io import generate_unique_filename

# COMMAND ----------
lm = LeadMantraREST()
dir_path = f"{lm.storage_paths.landing}/campaigns"
Path(f"/dbfs/{dir_path}").mkdir(exist_ok=True)

filepath = f"{dir_path}/{generate_unique_filename('campaigns')}.jsonl"
print("filepath:\t", filepath)
# COMMAND ----------
data = lm.get_campaigns()


with open(f"/dbfs/{filepath}", "w") as file:
    file.writelines([json.dumps(row) + "\n" for row in data])
# COMMAND ----------

df = spark.createDataFrame(data)
display(df)
