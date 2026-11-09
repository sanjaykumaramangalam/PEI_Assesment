# Databricks notebook source
# DBTITLE 1,Setup and read raw tables
import importlib, os, sys

sys.path.append(os.path.abspath("lib"))   # notebook cwd = src/
import sales_lib as sl
importlib.reload(sl)

SCHEMA = sl.SCHEMA   # set once in sales_lib.py
ENRICHED_CUSTOMER = f"{SCHEMA}.enriched_customer"
ENRICHED_PRODUCT  = f"{SCHEMA}.enriched_product"
ENRICHED_ORDER    = f"{SCHEMA}.enriched_order"

# Keep only well-formed rows (malformed rows are in _corrupt_record)
raw_customer = spark.table(f"{SCHEMA}.raw_customer")
raw_product = sl.drop_corrupt(spark.table(f"{SCHEMA}.raw_product"))
raw_order = sl.drop_corrupt(spark.table(f"{SCHEMA}.raw_order"))

# COMMAND ----------

# DBTITLE 1,Curate customers
customers = sl.curate_customers(raw_customer)
sl.write_delta(customers, ENRICHED_CUSTOMER)

# COMMAND ----------

# DBTITLE 1,Curate products
products = sl.curate_products(raw_product)
sl.write_delta(products, ENRICHED_PRODUCT)

# COMMAND ----------

# DBTITLE 1,Create enriched orders
orders = sl.enrich_orders(raw_order, spark.table(ENRICHED_CUSTOMER), spark.table(ENRICHED_PRODUCT))
sl.write_delta(orders, ENRICHED_ORDER)