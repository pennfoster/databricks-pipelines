# Databricks notebook source
# DBTITLE 1, Setup

import json
import logging
import time

from shared.classes import Orchestration

# COMMAND ------------------------
# DBTITLE 1, Instantiate Variables

orch = Orchestration()
jobs = orch.get_current_env_jobs()

running = ("RUNNING", None)
success = ("TERMINATED", "SUCCESS")
failed = [
    ("NEVER_RUN", None),
    ("INTERNAL_ERROR", "FAILED"),
    ("TERMINATED", "FAILED"),
    ("SKIPPED", None),
]

# DBTITLE 1, Relase if Integration Successful
# COMMAND ------------------------
# TODO: Better loop.
for attempt in range(10):
    # Get most recent CI/CD job runs
    run_statuses = orch.get_runs_statuses([job.id for job in jobs])
    if any(
        [(status not in [*failed, success, running]) for rid, status in run_statuses]
    ):
        logging.warning("unaccounted for job status in list:")
        logging.warning(set(run_statuses))
        raise ValueError("Unaccounted for job status")

    # If there are any failures halt notebook
    if any([(status in failed) for rid, status in run_statuses]):
        logging.warning(
            json.dumps(
                [(rid, status) for rid, status in run_statuses if status in failed],
                indent=2,
            )
        )
        # TODO: Alerting
        raise ValueError("One or more runs failed in Staging.")

    # If any jobs still running, sleep and await resolution
    if any([(status == running) for rid, status in run_statuses]):
        print("The following jobs are still running:")
        print(
            json.dumps(
                [rid for rid, status in run_statuses if status == running], indent=2
            )
        )
        print("sleeping...")
        time.sleep(30)
        print("retrying...")
        continue

    # If all recent runs are successful create release PR
    if all([(status == success) for rid, status in run_statuses]):
        orch.create_release("staging")
        # TODO: Alerting
        break
