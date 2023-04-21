import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from pytz import timezone

from shared.constants import COMPANY_TIMEZONE


def get_url_dataframe(env: str):
    df = pd.read_csv(
        f"/dbfs/mnt/sadataraw{env}001_control/supermetrics/urls_for_api_rev.csv"
    )
    return df


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
        file.write(json.dumps(data))

    return file_path


def load_json(file_path: str) -> json:
    with open(file_path, "r") as file:
        data = json.load(file)

    return data
