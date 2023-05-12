# Databricks notebook source
import os
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from pytz import timezone

from data_sources.supermetrics.classes import Supermetrics
from shared.classes.table_schemas import TableSchemas

# COMMAND -----
table_name = "bingcampaign"
ts = TableSchemas(data_source="supermetrics", table_name=table_name)
ts.create_table()
# COMMAND -----
# COMMAND -----
