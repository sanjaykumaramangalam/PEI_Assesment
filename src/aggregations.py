# Databricks notebook source
# DBTITLE 1,Build profit aggregate table
import importlib, os, sys

sys.path.append(os.path.abspath("lib"))   # notebook cwd = src/
import sales_lib as sl
importlib.reload(sl)

SCHEMA = sl.SCHEMA   # set once in sales_lib.py
AGG_TABLE = f"{SCHEMA}.agg_profit_by_year_category_customer"

orders = spark.table(f"{SCHEMA}.enriched_order")
agg = sl.aggregate_profit(orders)

sl.write_delta(agg, AGG_TABLE)