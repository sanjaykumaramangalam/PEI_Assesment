# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Sales Data Pipeline (PySpark)
# MAGIC
# MAGIC Read files -> save raw tables -> clean -> enrich -> aggregate, using only the PySpark DataFrame API.
# MAGIC The functions live in `../src/sales_transforms.py` and are unit tested in `../tests` (run `02_run_unit_tests`).
# MAGIC
# MAGIC | Step | Tables |
# MAGIC | --- | --- |
# MAGIC | 1. Raw | `raw_orders`, `raw_products`, `raw_customers` |
# MAGIC | 2. Enriched customers / products | `enriched_customers`, `enriched_products` |
# MAGIC | 3. Enriched orders | `enriched_orders` (profit rounded to 2 dp, customer name and country, category and sub-category) |
# MAGIC | 4. Aggregate | `agg_profit` (profit by year, category, sub-category, customer) |
# MAGIC | 5. Profit by | `profit_by_year`, `profit_by_year_category`, `profit_by_customer`, `profit_by_customer_year` |
# MAGIC
# MAGIC Set `target_schema` (catalog.schema) to save Delta tables. Leave it blank to use temp views.

# COMMAND ----------

# DBTITLE 1,Parameters
import os

PROJECT_ROOT = os.path.dirname(os.getcwd())  # notebook runs from Test_Data/notebooks

dbutils.widgets.text("data_path", f"file:{PROJECT_ROOT}/data", "Data folder")
dbutils.widgets.text("target_schema", "", "Target catalog.schema (blank = temp views)")

DATA_PATH = dbutils.widgets.get("data_path").rstrip("/")
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip() or None
print(f"data_path={DATA_PATH}\ntarget_schema={TARGET_SCHEMA or '(temp views)'}")

# COMMAND ----------

# DBTITLE 1,Import functions
import sys, importlib
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import sales_transforms as st
importlib.reload(st)   # pick up edits to the .py file without restarting
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Step 1: Read files and save raw tables
# Read each source file; column names are made Delta-safe ('Customer ID' -> 'customer_id')
raw_orders = st.standardize_columns(st.read_orders(spark, f"{DATA_PATH}/Orders.json"))
raw_products = st.standardize_columns(st.read_products(spark, f"{DATA_PATH}/Products.csv"))
raw_customers = st.standardize_columns(st.read_customers(spark, f"{DATA_PATH}/Customer.xlsx"))

tables = {}   # logical name -> saved table name
for name, df in {"raw_orders": raw_orders, "raw_products": raw_products, "raw_customers": raw_customers}.items():
    tables[name] = st.save_table(df, name, TARGET_SCHEMA)
    print(f"{tables[name]}: {spark.table(tables[name]).count()} rows")

# COMMAND ----------

# DBTITLE 1,Step 2: Enriched customers and products
customers = st.clean_customers(spark.table(tables["raw_customers"]))
products = st.clean_products(spark.table(tables["raw_products"]))
tables["enriched_customers"] = st.save_table(customers, "enriched_customers", TARGET_SCHEMA)
tables["enriched_products"] = st.save_table(products, "enriched_products", TARGET_SCHEMA)

print("customers:", spark.table(tables["enriched_customers"]).count())
print("products:", spark.table(tables["enriched_products"]).count(), f"(raw file has {raw_products.count()} rows; duplicate IDs removed)")

# Show the customer names that were cleaned
names = (raw_customers.select("customer_id", F.col("customer_name").alias("name_before"))
         .join(spark.table(tables["enriched_customers"]).select("customer_id", F.col("customer_name").alias("name_after")), "customer_id")
         .filter(F.col("name_before").isNull() | (F.col("name_before") != F.col("name_after"))))
print("customer names cleaned:", names.count())
display(names.limit(10))

# COMMAND ----------

# DBTITLE 1,Step 3: Enriched orders
# Order info + profit (2 dp) + customer name/country + product category/sub-category
orders = st.clean_orders(spark.table(tables["raw_orders"]))
enriched_orders = st.build_enriched_orders(
    orders, spark.table(tables["enriched_customers"]), spark.table(tables["enriched_products"]))
tables["enriched_orders"] = st.save_table(enriched_orders, "enriched_orders", TARGET_SCHEMA)

eo = spark.table(tables["enriched_orders"])
print(f"raw orders: {raw_orders.count()} | enriched orders: {eo.count()} | dropped as invalid: {raw_orders.count() - eo.count()}")
print("orders whose product is not in Products.csv (category = Unknown):", eo.filter(F.col("category") == st.UNKNOWN).count())
display(eo.limit(10))

# COMMAND ----------

# DBTITLE 1,Step 4: Aggregate table
# Profit by Year, Product Category, Product Sub Category and Customer
tables["agg_profit"] = st.save_table(st.build_profit_aggregate(eo), "agg_profit", TARGET_SCHEMA)
agg = spark.table(tables["agg_profit"])
display(agg.orderBy("order_year", "category", "sub_category", "customer_name"))

# COMMAND ----------

# DBTITLE 1,Step 5: Profit by year and customer
# The four requested aggregates, built from agg_profit with groupBy().agg()
rollups = {
    "profit_by_year": ["order_year"],
    "profit_by_year_category": ["order_year", "category"],
    "profit_by_customer": ["customer_id", "customer_name"],
    "profit_by_customer_year": ["customer_id", "customer_name", "order_year"],
}
for name, keys in rollups.items():
    tables[name] = st.save_table(st.profit_by(agg, *keys), name, TARGET_SCHEMA)
    print(f"{tables[name]}: {spark.table(tables[name]).count()} rows")

# COMMAND ----------

# DBTITLE 1,5a. Profit by Year
display(spark.table(tables["profit_by_year"]).orderBy("order_year"))

# COMMAND ----------

# DBTITLE 1,5b. Profit by Year + Product Category
display(spark.table(tables["profit_by_year_category"]).orderBy("order_year", "category"))

# COMMAND ----------

# DBTITLE 1,5c. Profit by Customer
display(spark.table(tables["profit_by_customer"]).orderBy(F.desc("total_profit"), "customer_id"))

# COMMAND ----------

# DBTITLE 1,5d. Profit by Customer + Year
display(spark.table(tables["profit_by_customer_year"]).orderBy("customer_id", "order_year"))

# COMMAND ----------

# DBTITLE 1,Check: total profit matches in every table
for name in ["enriched_orders", "agg_profit", *rollups]:
    col = "profit" if name == "enriched_orders" else "total_profit"
    total = spark.table(tables[name]).agg(F.round(F.sum(col), 2)).first()[0]
    print(f"{tables[name]:<25} total profit = {total}")