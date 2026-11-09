# Databricks notebook source
# DBTITLE 1,Profit by Year
# MAGIC %sql
# MAGIC SELECT
# MAGIC   year,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM cdp_uat.coe_aag_db.agg_profit_by_year_category_customer
# MAGIC GROUP BY year
# MAGIC ORDER BY year

# COMMAND ----------

# DBTITLE 1,Profit by Year and Product Category
# MAGIC %sql
# MAGIC SELECT
# MAGIC   year,
# MAGIC   category,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM cdp_uat.coe_aag_db.agg_profit_by_year_category_customer
# MAGIC GROUP BY year, category
# MAGIC ORDER BY year, category

# COMMAND ----------

# DBTITLE 1,Profit by Customer
# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM cdp_uat.coe_aag_db.agg_profit_by_year_category_customer
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC

# COMMAND ----------

# DBTITLE 1,Profit by Customer and Year
# MAGIC %sql
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   year,
# MAGIC   ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM cdp_uat.coe_aag_db.agg_profit_by_year_category_customer
# MAGIC GROUP BY customer_id, customer_name, year
# MAGIC ORDER BY customer_name, year