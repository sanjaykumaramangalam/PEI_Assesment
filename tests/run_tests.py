# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## Run the sales_lib unit tests
# MAGIC Runs `tests/test_sales_lib.py` with pytest. Run this before the pipeline notebooks; a non-zero exit fails the cell (and a job task).

# COMMAND ----------

# DBTITLE 1,Run pytest
import os
import sys
import pytest

# A notebook runs from its own folder, so the current folder is Assesment/tests
test_file = os.path.abspath("test_sales_lib.py")
lib_folder = os.path.abspath("../src/lib")

# Let the tests import sales_lib
sys.path.append(lib_folder)

# Workspace folders can't store __pycache__ files, so don't create them
sys.dont_write_bytecode = True

# Run the tests in test_sales_lib.py (-v shows each test result)
exit_code = pytest.main([test_file, "-v", "-p", "no:cacheprovider"])

# 0 means all tests passed; otherwise stop with an error
if exit_code != 0:
    raise Exception(f"Unit tests failed (pytest exit code {exit_code})")

# COMMAND ----------

