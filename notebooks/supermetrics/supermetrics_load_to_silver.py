# Databricks notebook source
import os
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from pytz import timezone

from data_sources.supermetrics.classes import Supermetrics
from data_sources.supermetrics.functions import get_url_dataframe, save_json, load_json
from shared.constants import COMPANY_TIMEZONE

# COMMAND -----
df = get_url_dataframe()
# COMMAND -----
indx = 1
row = df.iloc[1, :]
search_name = row["C001_SearchName"]
bronze_path = f"mnt/bronze/supermetrics/{search_name}"

from pathlib import Path

# p = Path("/mnt/bronze/supermetrics/FacebookAdSetV2")
paths = [a for a in Path(os.path.join("/dbfs/", bronze_path)).iterdir() if a.is_dir()]
# COMMAND -----
# TESTING
# TODO: Remove testing cell
dir = paths[0]
dir_mnt = Path(*dir.parts[2:])
