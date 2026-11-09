"""Unit tests for src/lib/sales_lib.py.

Each business rule of the pipeline gets small, hand-built DataFrames with a known
expected answer, including the edge cases found while profiling the real files:
duplicate product_ids, orders with unknown customer/product, junk/NULL customer
names, day-first dates and bad emails.

Run: tests/run_tests notebook, or `pytest tests/` from the Assesment folder.
"""

import datetime as dt

import os

import pytest
from databricks.sdk.runtime import spark   # the Databricks session (no new session created)

import sales_lib as sl

VOLUME = sl.SOURCE_DIR   # set once in sales_lib.py


# --------------------------------------------------------------------------------------
# Test data builders
# --------------------------------------------------------------------------------------
CUSTOMER_DDL = ("customer_id string, customer_name string, email string, phone string, address string, "
                "segment string, country string, city string, state string, postal_code string, region string")
PRODUCT_DDL = ("product_id string, category string, sub_category string, product_name string, "
               "state string, price_per_product double")
ORDER_DDL = ("row_id long, order_id string, order_date string, ship_date string, ship_mode string, "
             "customer_id string, product_id string, quantity long, price double, discount double, profit double")
ENRICHED_DDL = ("row_id long, order_id string, order_date date, ship_date date, customer_id string, "
                "customer_name string, category string, sub_category string, quantity long, "
                "discount double, profit double")


def customer(cid, name, email="a@b.com", phone="555-123-4567", country="United States"):
    return (cid, name, email, phone, "1 Main St", "Consumer", country, "Austin", "Texas", "78701", "Central")


def order(row_id, cust="C1", prod="P1", od="21/8/2016", sd="25/8/2016", profit=10.0, order_id=None):
    return (row_id, order_id or f"O{row_id}", od, sd, "Standard Class", cust, prod, 2, 100.0, 0.1, profit)


def rows_by(df, key):
    return {r[key]: r for r in df.collect()}


# --------------------------------------------------------------------------------------
# clean_person_name (native Spark column expression)
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize("raw, expected", [
    ("Jay Kimmel", "Jay Kimmel"),                       # clean name unchanged
    ("Bi1l Donatelli", "Bill Donatelli"),               # 1 -> l
    ("N0ra Paige", "Nora Paige"),                       # 0 -> o
    ("Ana Wa55erman", "Ana Wasserman"),                 # 55 -> ss
    ("C@thy Armstrong", "Cathy Armstrong"),             # @ -> a
    ("Dan!els", "Daniels"),                             # ! -> i
    ("Ji11 Stevenson", "Jill Stevenson"),               # trailing look-alikes
    ("Maribeth 5chnelling", "Maribeth Schnelling"),     # leading look-alike
    ("Tam&^*ara Willingham", "Tamara Willingham"),      # junk inside a word
    ("Anth 34789ony Rawles", "Anthony Rawles"),         # word split by digits
    ("B     ecky Martin", "Becky Martin"),              # word split by spaces
    ("  Jay   Kimmel  ", "Jay Kimmel"),                 # extra outer/inner spaces
    ("jay kimmel", "Jay Kimmel"),                       # capitalisation
    ("O'Rourke", "O'Rourke"),                           # apostrophe kept
    ("Corey-Lock", "Corey-Lock"),                       # hyphen kept
    ("Müller", "Müller"),                               # unicode letters kept
])
def test_clean_person_name(raw, expected):
    df = spark.createDataFrame([(raw,)], "name string")
    assert df.select(sl.clean_person_name("name")).first()[0] == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "123", "#$%"])
def test_clean_person_name_returns_none_when_no_letters(raw):
    df = spark.createDataFrame([(raw,)], "name string")
    assert df.select(sl.clean_person_name("name")).first()[0] is None


# --------------------------------------------------------------------------------------
# Generic helpers
# --------------------------------------------------------------------------------------
def test_clean_columns_snake_case_and_keeps_metadata():
    df = spark.createDataFrame([(1, 2, 3, 4)], ["Product ID", "Sub-Category", "Postal Code ", "_corrupt_record"])
    assert sl.clean_columns(df).columns == ["product_id", "sub_category", "postal_code", "_corrupt_record"]


def test_require_columns_passes_and_fails():
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    sl.require_columns(df, ["a", "b"], "t")  # no error
    with pytest.raises(ValueError, match=r"missing expected columns \['c'\]"):
        sl.require_columns(df, ["a", "c"], "t")


def test_check_source_file_missing_and_empty(tmp_path):
    with pytest.raises(FileNotFoundError):
        sl.check_source_file(str(tmp_path / "nope.csv"))
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(ValueError, match="empty"):
        sl.check_source_file(str(empty))
    ok = tmp_path / "ok.csv"
    ok.write_text("a,b\n1,2\n")
    sl.check_source_file(str(ok))  # no error


def test_drop_corrupt():
    df = spark.createDataFrame([(1, None), (2, "bad line")], "id int, _corrupt_record string")
    good = sl.drop_corrupt(df)
    assert [r.id for r in good.collect()] == [1]
    assert sl.CORRUPT_COL not in good.columns


def test_drop_corrupt_without_corrupt_column():
    df = spark.createDataFrame([(1,), (2,)], "id int")
    assert sl.drop_corrupt(df).count() == 2


# --------------------------------------------------------------------------------------
# Customers
# --------------------------------------------------------------------------------------
def test_curate_customers():
    raw = spark.createDataFrame([
        customer(" C1 ", "Bi1l Donatelli", email="  Bill@Mail.COM "),
        customer("C2", None, email="not-an-email", phone="123"),
        customer("C3", "%%", country=" "),
        customer("C4", "Jay Kimmel"),
        customer("C4", None),          # duplicate id -> the named row wins
        customer("", "No Id"),          # blank key -> dropped
    ], CUSTOMER_DDL)
    out = sl.curate_customers(raw)
    rows = rows_by(out, "customer_id")

    assert sorted(rows) == ["C1", "C2", "C3", "C4"]
    assert out.count() == 4                               # unique on customer_id
    c1, c2, c3, c4 = rows["C1"], rows["C2"], rows["C3"], rows["C4"]
    assert (c1.customer_name, c1.is_name_cleaned, c1.is_name_missing) == ("Bill Donatelli", True, False)
    assert (c1.email, c1.is_valid_email) == ("bill@mail.com", True)
    assert (c2.customer_name, c2.is_name_missing) == ("Unknown", True)
    assert c2.is_valid_email is False
    assert c3.customer_name == "Unknown" and c3.country == "Unknown"
    assert (c4.customer_name, c4.is_name_cleaned) == ("Jay Kimmel", False)


def test_curate_customers_missing_column_fails():
    raw = spark.createDataFrame([("C1",)], "customer_id string")
    with pytest.raises(ValueError, match="raw_customer"):
        sl.curate_customers(raw)


# --------------------------------------------------------------------------------------
# Products
# --------------------------------------------------------------------------------------
def test_curate_products_dedup_and_nulls():
    raw = spark.createDataFrame([
        ("P1", "Furniture", "Chairs", "Zeta Chair", "Texas", 10.0),
        ("P1", "Furniture", "Chairs", "Alpha Chair", "Ohio", 12.0),   # kept: first by name
        (" P2 ", None, " ", None, "Utah", 5.4567),
        (None, "X", "Y", "No id", "Utah", 1.0),                       # dropped
    ], PRODUCT_DDL)
    rows = rows_by(sl.curate_products(raw), "product_id")

    assert sorted(rows) == ["P1", "P2"]
    assert rows["P1"].product_name == "Alpha Chair"
    assert (rows["P2"].category, rows["P2"].sub_category, rows["P2"].product_name) == ("Unknown",) * 3
    assert rows["P2"].price_per_product == 5.46


# --------------------------------------------------------------------------------------
# Enriched orders
# --------------------------------------------------------------------------------------
@pytest.fixture
def dims():
    customers = spark.createDataFrame([("C1", "Jay Kimmel", "United States")],
                                      "customer_id string, customer_name string, country string")
    products = spark.createDataFrame([("P1", "Furniture", "Chairs")],
                                     "product_id string, category string, sub_category string")
    return customers, products


def test_enrich_orders(dims):
    orders = spark.createDataFrame([
        order(1, profit=63.6853),
        order(2, cust="C9", prod="P9", profit=-1.005),     # unknown customer and product
        order(3, od="3/1/2017", sd="7/1/2017"),            # day-first: 3 Jan, not 1 Mar
        order(4, od="31/2/2016"),                          # impossible date -> NULL
    ], ORDER_DDL)
    out = sl.enrich_orders(orders, *dims)
    rows = rows_by(out, "row_id")

    assert out.count() == 4                                # left joins keep every order
    r1, r2, r3, r4 = rows[1], rows[2], rows[3], rows[4]
    assert (r1.customer_name, r1.country, r1.category, r1.sub_category) == ("Jay Kimmel", "United States", "Furniture", "Chairs")
    assert r1.profit == 63.69
    assert (r1.order_date, r1.ship_date) == (dt.date(2016, 8, 21), dt.date(2016, 8, 25))
    assert (r1.customer_found, r1.product_found) == (True, True)
    assert (r2.customer_name, r2.country, r2.category, r2.sub_category) == ("Unknown",) * 4
    assert (r2.customer_found, r2.product_found) == (False, False)
    assert r3.order_date == dt.date(2017, 1, 3)
    assert r4.order_date is None


def test_enrich_orders_unique_dims_do_not_multiply_rows(dims):
    customers, _ = dims
    products = sl.curate_products(spark.createDataFrame([
        ("P1", "Furniture", "Chairs", "A", "Texas", 1.0),
        ("P1", "Furniture", "Chairs", "B", "Ohio", 1.0),
    ], PRODUCT_DDL))
    orders = spark.createDataFrame([order(1), order(2)], ORDER_DDL)
    assert sl.enrich_orders(orders, customers, products).count() == 2


# --------------------------------------------------------------------------------------
# Aggregates
# --------------------------------------------------------------------------------------
def enriched(rows):
    return spark.createDataFrame(rows, ENRICHED_DDL)


GOOD = (1, "O1", dt.date(2016, 1, 1), dt.date(2016, 1, 3), "C1", "A", "F", "S", 2, 0.2, 5.0)


def test_aggregate_profit():
    df = enriched([
        (1, "O1", dt.date(2016, 1, 1), dt.date(2016, 1, 2), "C1", "A", "F", "S", 1, 0.0, 1.111),
        (2, "O1", dt.date(2016, 1, 1), dt.date(2016, 1, 2), "C1", "A", "F", "S", 1, 0.0, 2.222),  # same order
        (3, "O2", dt.date(2016, 5, 1), dt.date(2016, 5, 2), "C1", "A", "F", "S", 1, 0.0, -0.333),
        (4, "O3", dt.date(2017, 1, 1), dt.date(2017, 1, 2), "C1", "A", "F", "S", 1, 0.0, 4.0),   # other year
        (5, "O4", dt.date(2016, 1, 1), dt.date(2016, 1, 2), "C2", "B", "F", "S", 1, 0.0, 7.0),   # other customer
    ])
    out = {(r.year, r.customer_id): (r.total_profit, r.order_count) for r in sl.aggregate_profit(df).collect()}
    assert out == {(2016, "C1"): (3.0, 2), (2017, "C1"): (4.0, 1), (2016, "C2"): (7.0, 1)}


def test_aggregate_profit_total_matches_input():
    df = enriched([GOOD, (2,) + GOOD[1:4] + ("C2", "B", "T", "X", 1, 0.0, -2.5)])
    total = sum(r.total_profit for r in sl.aggregate_profit(df).collect())
    assert total == pytest.approx(2.5)


# --------------------------------------------------------------------------------------
# Integration: real source files (skipped when the volume is not reachable)
# --------------------------------------------------------------------------------------
needs_volume = pytest.mark.skipif(not os.path.exists(VOLUME), reason="source volume not available")


@needs_volume
@pytest.mark.parametrize("reader, file, expected", [
    ("read_products", "Products.csv", 1851),
    ("read_customers", "Customer.xlsx", 793),
    ("read_orders", "Orders.json", 9994),
])
def test_readers_load_all_rows(reader, file, expected):
    df = getattr(sl, reader)(spark, VOLUME + file)
    assert df.count() == expected
