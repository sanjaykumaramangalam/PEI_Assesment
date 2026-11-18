# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC # Data quality checks on the real tables
# MAGIC Unit tests (`02_run_unit_tests`) prove the logic with small test data. This notebook applies the same rules to the **real** tables after each load.
# MAGIC
# MAGIC | Level | Meaning | Action when it fails |
# MAGIC | --- | --- | --- |
# MAGIC | **Critical** | Data cannot be trusted (missing file/column, empty table, totals do not reconcile, too many rejects) | Job **fails**, so later tasks (reports) do not run and the team is alerted |
# MAGIC | **Reject** | Row breaks a required rule (missing id, invalid date, bad profit) | Row saved to `rejected_orders` with a `reject_reason`; good rows continue |
# MAGIC | **Warn** | Row is usable but not perfect (customer/product not found, blank name, bad ship date) | Row kept as `Unknown` / NULL and counted |
# MAGIC
# MAGIC Every rule result is saved to `dq_results` (one row per rule per run).
# MAGIC
# MAGIC Run order in a job: `02_run_unit_tests` -> `01_sales_pipeline` -> `03_data_quality_checks`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os

PROJECT_ROOT = os.path.dirname(os.getcwd())  # notebook runs from Test_Data/notebooks

dbutils.widgets.text("data_path", f"file:{PROJECT_ROOT}/data", "Data folder")
dbutils.widgets.text("target_schema", "", "Target catalog.schema (blank = temp views)")
dbutils.widgets.text("max_reject_pct", "5", "Max % of orders allowed to be rejected")

DATA_PATH = dbutils.widgets.get("data_path").rstrip("/")
TARGET_SCHEMA = dbutils.widgets.get("target_schema").strip() or None
MAX_REJECT_PCT = float(dbutils.widgets.get("max_reject_pct"))
print(f"data_path={DATA_PATH}\ntarget_schema={TARGET_SCHEMA or '(temp views)'}\nmax_reject_pct={MAX_REJECT_PCT}")

# COMMAND ----------

# DBTITLE 1,Import functions and check helper
import sys, importlib, datetime
sys.dont_write_bytecode = True
sys.path.insert(0, os.path.join(PROJECT_ROOT, "src"))

import sales_transforms as st
importlib.reload(st)   # pick up edits to the .py file without restarting
from pyspark.sql import functions as F

RUN_TIME = datetime.datetime.now()
results = []   # one entry per rule


def check(table, rule, level, failed_rows, total_rows, details=""):
    """Record one rule result. level = Critical / Reject / Warn."""
    status = "PASS" if failed_rows == 0 else "FAIL"
    results.append((RUN_TIME, table, rule, level, int(failed_rows), int(total_rows), status, details))
    print(f"{status:4} | {level:8} | {table:18} | {rule:40} | failed={failed_rows} of {total_rows} {details}")


def save_results():
    """Save all rule results. With a schema the history is kept (append); otherwise a temp view."""
    df = spark.createDataFrame(results, "run_time timestamp, table_name string, rule string, level string, "
                                        "failed_rows long, total_rows long, status string, details string")
    if TARGET_SCHEMA:
        df.write.mode("append").saveAsTable(f"{TARGET_SCHEMA}.dq_results")
    else:
        df.createOrReplaceTempView("dq_results")
    return df


def critical_failures():
    return [r for r in results if r[3] == "Critical" and r[6] == "FAIL"]

# COMMAND ----------

# DBTITLE 1,Step 1: Load raw tables
# With a target schema, check the tables saved by 01_sales_pipeline.
# Without one, read the source files directly (a missing file fails here = Critical).
if TARGET_SCHEMA:
    raw_orders = spark.table(f"{TARGET_SCHEMA}.raw_orders")
    raw_customers = spark.table(f"{TARGET_SCHEMA}.raw_customers")
    raw_products = spark.table(f"{TARGET_SCHEMA}.raw_products")
else:
    raw_orders = st.standardize_columns(st.read_orders(spark, f"{DATA_PATH}/Orders.json"))
    raw_customers = st.standardize_columns(st.read_customers(spark, f"{DATA_PATH}/Customer.xlsx"))
    raw_products = st.standardize_columns(st.read_products(spark, f"{DATA_PATH}/Products.csv"))

raw_tables = {"raw_orders": raw_orders, "raw_customers": raw_customers, "raw_products": raw_products}
raw_counts = {name: df.count() for name, df in raw_tables.items()}
print(raw_counts)

# COMMAND ----------

# DBTITLE 1,Step 2: Critical checks - empty table and required columns
REQUIRED_COLUMNS = {
    "raw_orders": ["row_id", "order_id", "order_date", "ship_date", "ship_mode", "customer_id",
                   "product_id", "quantity", "price", "discount", "profit"],
    "raw_customers": ["customer_id", "customer_name", "country"],
    "raw_products": ["product_id", "category", "sub_category", "price_per_product"],
}

for name, df in raw_tables.items():
    # table must have rows
    check(name, "table is not empty", "Critical", 1 if raw_counts[name] == 0 else 0, raw_counts[name])
    # source file must still have every column we use (schema change)
    missing = [c for c in REQUIRED_COLUMNS[name] if c not in df.columns]
    check(name, "required columns present", "Critical", len(missing), len(REQUIRED_COLUMNS[name]),
          f"missing: {missing}" if missing else "")

# Later steps cannot run without these, so stop now if any failed
if critical_failures():
    save_results()
    raise Exception(f"Data quality failed (Critical): {critical_failures()}")

# COMMAND ----------

# DBTITLE 1,Step 3: Reject checks - save bad orders with a reason
total_orders = raw_counts["raw_orders"]

# Tag every raw order with the reason it would be dropped (NULL = valid)
flagged = st.add_reject_reason(raw_orders)
rejected_orders = flagged.filter(F.col("reject_reason").isNotNull()).withColumn("checked_at", F.lit(RUN_TIME))
REJECTED_TABLE = st.save_table(rejected_orders, "rejected_orders", TARGET_SCHEMA)

# One rule result per reason (0 failures is recorded too, so every rule shows in the report)
reason_counts = {r.reject_reason: r["count"] for r in rejected_orders.groupBy("reject_reason").count().collect()}
for reason in ["missing order id", "missing customer id", "missing product id", "invalid order date", "invalid profit"]:
    check("raw_orders", reason, "Reject", reason_counts.get(reason, 0), total_orders)

# Same row_id sent more than once (only the first is kept)
valid_orders = flagged.filter(F.col("reject_reason").isNull())
duplicate_orders = valid_orders.count() - valid_orders.dropDuplicates(["row_id"]).count()
check("raw_orders", "duplicate row_id", "Reject", duplicate_orders, total_orders)

# Too many rejects means a bad file, not a few bad rows -> Critical
rejected_count = sum(reason_counts.values())
reject_pct = round(100 * rejected_count / total_orders, 2)
check("raw_orders", f"rejected rows <= {MAX_REJECT_PCT}%", "Critical",
      rejected_count if reject_pct > MAX_REJECT_PCT else 0, total_orders, f"rejected = {reject_pct}%")

# Customers / products with a blank or missing id are dropped
for name, id_col in [("raw_customers", "customer_id"), ("raw_products", "product_id")]:
    blank_ids = raw_tables[name].filter(st.clean_id(id_col).isNull()).count()
    check(name, f"missing {id_col}", "Reject", blank_ids, raw_counts[name])

print(f"\nRejected orders saved to: {REJECTED_TABLE}")
display(spark.table(REJECTED_TABLE).limit(20))

# COMMAND ----------

# DBTITLE 1,Step 4: Build or load cleaned and enriched tables
if TARGET_SCHEMA:
    customers = spark.table(f"{TARGET_SCHEMA}.enriched_customers")
    products = spark.table(f"{TARGET_SCHEMA}.enriched_products")
    enriched_orders = spark.table(f"{TARGET_SCHEMA}.enriched_orders")
    agg = spark.table(f"{TARGET_SCHEMA}.agg_profit")
else:
    customers = st.clean_customers(raw_customers)
    products = st.clean_products(raw_products)
    enriched_orders = st.build_enriched_orders(st.clean_orders(raw_orders), customers, products)
    agg = st.build_profit_aggregate(enriched_orders)

enriched_count = enriched_orders.count()
print(f"customers={customers.count()} products={products.count()} enriched_orders={enriched_count}")

# COMMAND ----------

# DBTITLE 1,Step 5: Warn checks - kept but flagged
# Orders whose customer / product is not in the master file (kept as 'Unknown')
no_customer = enriched_orders.join(customers.select("customer_id"), "customer_id", "left_anti").count()
no_product = enriched_orders.join(products.select("product_id"), "product_id", "left_anti").count()
check("enriched_orders", "customer not found (Unknown)", "Warn", no_customer, enriched_count)
check("enriched_orders", "product not found (Unknown)", "Warn", no_product, enriched_count)

# Optional order fields that could not be read (kept as NULL)
bad_ship_date = valid_orders.filter(F.col("ship_date").isNotNull() & st.to_date_dmy("ship_date").isNull()).count()
bad_quantity = valid_orders.filter(F.col("quantity").isNotNull()
                                   & F.expr("try_cast(quantity AS INT)").isNull()).count()
check("raw_orders", "invalid ship date (NULL)", "Warn", bad_ship_date, total_orders)
check("raw_orders", "invalid quantity (NULL)", "Warn", bad_quantity, total_orders)

# Master data with blank values (set to 'Unknown')
customer_count = customers.count()
check("enriched_customers", "customer name Unknown", "Warn",
      customers.filter(F.col("customer_name") == st.UNKNOWN).count(), customer_count)
check("enriched_customers", "country Unknown", "Warn",
      customers.filter(F.col("country") == st.UNKNOWN).count(), customer_count)

# Product ids repeated in the source file (one row kept)
product_dups = raw_counts["raw_products"] - raw_products.select(st.clean_id("product_id").alias("id")).distinct().count()
check("raw_products", "duplicate product_id in source", "Warn", product_dups, raw_counts["raw_products"])

# COMMAND ----------

# DBTITLE 1,Step 6: Critical checks - keys and reconciliation
# Keys must be unique after cleaning (otherwise joins multiply rows)
for name, df, key in [("enriched_customers", customers, "customer_id"),
                      ("enriched_products", products, "product_id"),
                      ("enriched_orders", enriched_orders, "row_id")]:
    rows = df.count()
    check(name, f"{key} is unique", "Critical", rows - df.select(key).distinct().count(), rows)

# No NULL keys in the final orders table
null_keys = enriched_orders.filter(F.col("order_id").isNull() | F.col("customer_id").isNull()
                                   | F.col("product_id").isNull() | F.col("order_year").isNull()).count()
check("enriched_orders", "no NULL keys", "Critical", null_keys, enriched_count)

# Every raw order is accounted for: raw = loaded + rejected + duplicates
unaccounted = total_orders - (enriched_count + rejected_count + duplicate_orders)
check("enriched_orders", "raw = loaded + rejected + duplicates", "Critical", abs(unaccounted), total_orders,
      f"raw={total_orders} loaded={enriched_count} rejected={rejected_count} duplicates={duplicate_orders}")

# Total profit is the same in the enriched table and the aggregate table
profit_enriched = float(enriched_orders.agg(F.sum("profit")).first()[0] or 0)
profit_agg = float(agg.agg(F.sum("total_profit")).first()[0] or 0)
profit_by_year = float(st.profit_by(agg, "order_year").agg(F.sum("total_profit")).first()[0] or 0)
profit_ok = abs(profit_enriched - profit_agg) <= 0.01 and abs(profit_enriched - profit_by_year) <= 0.01
check("agg_profit", "total profit reconciles", "Critical", 0 if profit_ok else 1, 1,
      f"enriched={profit_enriched:.2f} agg={profit_agg:.2f} by_year={profit_by_year:.2f}")

# COMMAND ----------

# DBTITLE 1,Step 7: Save results to dq_results
dq_results = save_results()
print(f"Saved {dq_results.count()} rule results to {f'{TARGET_SCHEMA}.dq_results' if TARGET_SCHEMA else 'dq_results (temp view)'}")
display(dq_results.orderBy(F.desc("status"), "level", "table_name", "rule"))

# COMMAND ----------

# DBTITLE 1,Step 8: Fail the job on Critical failures
summary = {level: sum(1 for r in results if r[3] == level and r[6] == "FAIL") for level in ["Critical", "Reject", "Warn"]}
print(f"Rules checked: {len(results)} | failed by level: {summary}")

# Reject and Warn failures are reported only; Critical failures stop the job
if critical_failures():
    raise Exception(f"Data quality failed (Critical): {critical_failures()}")
print("Data quality passed: no Critical failures.")