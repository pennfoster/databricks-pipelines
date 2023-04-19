# Databricks notebook source


dbutils.widgets.text("job_ids", "[]")

ids = dbutils.widgets.get("job_ids")

print(ids)
# import aiohttp

# job_id_list = []


# async def trigger_job_coro(async_session: aiohttp.ClientSession, job_settings: str):
#     pass
