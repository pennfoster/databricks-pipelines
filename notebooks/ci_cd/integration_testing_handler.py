# Databricks notebook source
import json

# COMMAND ------------------------
dbutils.widgets.text("jobs_in_test", default="[]")

jobs_in_test = json.loads(dbutils.widgets.get("jobs_in_test"))

# COMMAND ------------------------
# while jobs in test are running - sleep

# COMMAND ------------------------
# if jobs in test fail - send an email?

# COMMAND ------------------------
# if jobs in test succeed
# * check that no jobs are running
# * check that no recent failures exist in staging
# * create new release/** branch
# * create new PR to Master
