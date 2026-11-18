"""Shared pytest fixtures for the sales transformation tests."""
import os
import sys

import pytest

sys.dont_write_bytecode = True  # avoid writing .pyc files into the workspace
_SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)


@pytest.fixture(scope="session")
def spark():
    """Reuse the active Databricks session (serverless / classic) or build a local one."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.master("local[2]").appName("sales-tests").getOrCreate()
