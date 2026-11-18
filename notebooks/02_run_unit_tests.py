# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC # Run unit tests
# MAGIC Runs `../tests` with pytest on the current Spark session. The notebook fails (raises) if any test fails, so it can be used as a CI gate or as a Job task before `01_sales_pipeline`.

# COMMAND ----------

# DBTITLE 1,Install pytest if missing
# MAGIC %pip install -q pytest

# COMMAND ----------

# DBTITLE 1,Run pytest
import os, sys, pytest
sys.dont_write_bytecode = True
TESTS = os.path.join(os.path.dirname(os.getcwd()), "tests")

# drop cached modules so edits to src/tests are always picked up
for m in [k for k in list(sys.modules) if k.startswith(("sales_transforms", "conftest", "test_sales"))]:
    del sys.modules[m]

rc = pytest.main([TESTS, "-v", "-p", "no:cacheprovider", "--tb=short", "-rfE"])
assert rc == 0, f"Unit tests failed (pytest exit code {rc})"

# COMMAND ----------

