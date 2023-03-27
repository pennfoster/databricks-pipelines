# Databricks notebook source
import json
from pathlib import Path

import pendulum

from data_sources.marketo.classes import MarketoREST
from shared.functions.file_io import generate_unique_filename

# COMMAND ----------
assets = {
    # api_object: table_name
    "Get all assets": "",
    "channels": "channels",
    "emails": "emails",
    "emailTemplates": "email_templates",
    "form/fields": "form_fields",
    "forms": "forms",
    "landingPages": "landing_pages",
    "programs": "programs",
    "smartCampaigns": "smart_campaigns",
    "smartLists": "smart_lists",
    "snippets": "snippets",
    "staticLists": "static_lists",
}
# COMMAND ----------
dbutils.widgets.dropdown(
    "asset", list(assets.keys())[0], list(assets.keys()), "1. Asset"
)
dbutils.widgets.text(
    name="start_date",
    defaultValue=str(pendulum.now().date().subtract(days=1)),
    label="2. Start date (YYYY-MM-DD)",
)
# COMMAND ----------
w_asset = dbutils.widgets.get("asset")
w_start_date = dbutils.widgets.get("start_date")
if w_asset == "Get all assets":
    assets.pop(w_asset)
else:
    assets = {w_asset: assets[w_asset]}

# COMMAND ----------

for api_object, table_name in assets.items():
    marketo = MarketoREST()
    dir_path = f"{marketo.storage_paths.landing}/{table_name}"
    Path(f"/dbfs/{dir_path}").mkdir(exist_ok=True)

    filename = generate_unique_filename(table_name)
    filepath = f"{dir_path}/{filename}.jsonl"

    offset = 0
    with open(f"/dbfs/{filepath}", "a") as file:
        while True:
            results, warnings, errors = marketo.get_assets_data(
                api_object=api_object, start_date=w_start_date, offset=offset
            )
            if len(results) == 0:
                break
            offset += len(results)
            file.writelines((json.dumps(row) + "\n" for row in results))
            print(f"Rows retrieved:\t{offset}")

    df = spark.read.json(filepath)
    if offset != df.count():
        raise ValueError("Row count pulled from API does not match rows read from file")
