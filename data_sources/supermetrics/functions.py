import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from pytz import timezone

from shared.constants import COMPANY_TIMEZONE
from shared.functions.databricks_utilities import pass_databricks_env


@pass_databricks_env
def get_url_dataframe(**kwargs):
    env = kwargs["env"]
    df = pd.read_csv(
        f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_api_rev.csv"
    )
    return df


def supermetrics_silver_widgets_config():
    import os
    import pandas as pd
    import re
    from datetime import datetime, timedelta
    from pathlib import Path
    from pytz import timezone

    from data_sources.supermetrics.classes import Supermetrics
    from shared.classes.table_schemas import TableSchemas
    from shared.functions.azure_utilities import get_mount_paths

    # from data_sources.supermetrics.functions import (
    #     get_url_dataframe,
    #     save_json,
    #     load_json,
    # )
    from shared.constants import COMPANY_TIMEZONE

    spark.conf.set("spark.sql.ansi.enabled", True)

    tz = timezone(COMPANY_TIMEZONE)
    seven_days_ago = (datetime.now(tz) - timedelta(days=7)).strftime("%Y-%m-%d")

    dbutils.widgets.text("end_date", datetime.now(tz).strftime("%Y-%m-%d"), "")
    dbutils.widgets.combobox(
        "start_date", seven_days_ago, [seven_days_ago, "1900-01-01"]
    )

    search_name = "BingAdGroup"
    url_df = get_url_dataframe()
    query_list = (
        url_df[url_df["C001_SearchName"] == search_name]["C001_QueryName"]
        .sort_values()
        .to_list()
    )
    query_list += ["All"]
    dbutils.widgets.dropdown("query_name", "All", query_list)

    # COMMAND -----
    query_name = dbutils.widgets.get("query_name")
    if query_name == "All":
        query_list.remove("All")
    else:
        query_list = [query_name]

    start_date = dbutils.widgets.get("start_date")
    end_date = dbutils.widgets.get("end_date")

    where_clause_start = f"where date >= '{start_date}'"
    if end_date:
        where_clause_end = f"and date <= '{end_date}'"

    return search_name, query_name, where_clause_start, where_clause_end


def save_json(
    dest_dir: str, file_name: str, data: json, suffix: str = None, parents: bool = False
) -> str:
    Path(dest_dir).mkdir(parents=parents, exist_ok=True)
    file_name = file_name.replace(".json", "")
    if suffix == "timestamp":
        tz = timezone(COMPANY_TIMEZONE)
        file_name += f'_{datetime.now(tz).strftime("%Y%m%d-%H%M%S")}'

    file_path = f"{dest_dir}/{file_name}.json"
    with open(f"{dest_dir}/{file_name}.json", "w") as file:
        # file.write(json.dumps(data))
        # with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, file)  # , ensure_ascii=False, indent=4)

    return file_path


def load_json(file_path: str) -> json:
    with open(file_path, "r") as file:
        data = json.load(file)

    return data
