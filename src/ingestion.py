# Databricks notebook source
# DBTITLE 1,Setup and config
import importlib, os, sys

sys.path.append(os.path.abspath("lib"))   # notebook cwd = src/
import sales_lib as sl
importlib.reload(sl)

# Source folder and schema are set once in sales_lib.py
SOURCE_DIR = sl.SOURCE_DIR
SCHEMA = sl.SCHEMA

# source name -> (reader, file, target table)
SOURCES = {
    "product":  (sl.read_products,  "Products.csv",  f"{SCHEMA}.raw_product"),
    "customer": (sl.read_customers, "Customer.xlsx", f"{SCHEMA}.raw_customer"),
    "order":    (sl.read_orders,    "Orders.json",   f"{SCHEMA}.raw_order"),
}

# COMMAND ----------

# DBTITLE 1,Load raw Delta tables
results = []
for name, (reader, file, table) in SOURCES.items():
    try:
        df = reader(spark, SOURCE_DIR + file)
        # audit columns first: _metadata is only available directly on the file scan
        df = sl.clean_columns(sl.add_audit_columns(df)).drop("_metadata")
        results.append((table, file, sl.write_delta(df, table)))
    except Exception as e:
        sl.logger.error(f"Ingestion failed for {file} -> {table}: {e}")
        raise

display(spark.createDataFrame(results, "table string, source_file string, row_count long"))