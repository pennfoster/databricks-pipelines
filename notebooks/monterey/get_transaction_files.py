# Databricks notebook source
import logging, re
from pathlib import Path

import pendulum

from shared.classes import SFTPBase
from shared.functions.azure_utilities import get_key_vault_scope

# COMMAND ----------
sftp_vars = {
    "host": "ftp.montereyfinancial.com",
    "username": "careerstp",
    "password": dbutils.secrets.get(get_key_vault_scope(), "monterey-sftp-pw"),
    "hostkeys": None,
}
failures = {}

# COMMAND ----------
with SFTPBase(**sftp_vars, data_source="monterey") as sftp:
    host_directory = "./Transactions"

    landing_directory = sftp.storage_paths.landing
    Path(f"/dbfs/{landing_directory}").mkdir(exist_ok=True)

    remote_file_list = sftp.connection.listdir(host_directory)

    pattern = r"^(?P<table>Transactions|Declines)_?(?P<date>\d{4}-?\d{2}-?\d{2})_?(?P<time>\d{2}-?\d{2}-?\d{2})?\.csv$"
    for remote_file in remote_file_list:
        m = re.match(pattern, remote_file)
        if not m:
            logging.warning("file name found not as expected: %s" % remote_file)
            failures[
                "sftp_name_pattern"
            ] = "One or more remote files do not conform to expected name pattern."
            continue

        table = m["table"]
        date = m["date"]

        table_dir = f"{landing_directory}/{table}"
        Path(f"/dbfs/{table_dir}").mkdir(exist_ok=True)
        processed_dir = f"{table_dir}/processed"
        Path(f"/dbfs/{processed_dir}").mkdir(exist_ok=True)
        local_files = [
            *[file.name for file in dbutils.fs.ls(table_dir)],
            *[file.name for file in dbutils.fs.ls(processed_dir)],
        ]
        if remote_file in local_files:
            continue

        print(
            f"{pendulum.now().time()}\tcopying {remote_file}... ({sftp.connection.lstat(f'{host_directory}/{remote_file}').st_size} bytes)"
        )
        try:
            sftp.extract_file_to_raw(
                host_dir=host_directory,
                host_file=remote_file,
                local_subdir=table,
            )
        except Exception as e:
            dbutils.fs.rm(f"{table_dir}/{remote_file}")
            logging.warning(e)
            failures["load_error"] = (
                "One or more file downloads encountered an error."
                "This file has been removed locally."
            )

# COMMAND ----------
if any(failures):
    import json

    raise Exception(json.dumps(failures))
