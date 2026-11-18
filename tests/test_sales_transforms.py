"""Unit tests for sales_transforms.

Each test uses a tiny in-memory DataFrame with a known answer.
Run with notebooks/02_run_unit_tests.

Each section has positive tests (good data gives the right answer) and
negative tests (bad, blank, missing or unexpected data is handled safely).
"""
import datetime as dt
import os

import pytest
from pyspark.sql import functions as F

import sales_transforms as st

ORDER_SCHEMA = ("row_id string, order_id string, order_date string, ship_date string, ship_mode string, "
                "customer_id string, product_id string, quantity string, price string, discount string, "
                "profit string")
DATA = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))


def order(row_id="1", order_id="O1", order_date="21/8/2016", ship_date="25/8/2016",
          customer_id="C1", product_id="P1", quantity="2", profit="10.00"):
    """One order line with sensible defaults; override only what a test cares about."""
    return (row_id, order_id, order_date, ship_date, "Standard Class",
            customer_id, product_id, quantity, "100", "0.2", profit)


@pytest.fixture
def customers(spark):
    return st.clean_customers(spark.createDataFrame(
        [("C1", "Alex Avila", "United States"), ("C2", "Ben Wallace", "United States")],
        "customer_id string, customer_name string, country string"))


@pytest.fixture
def products(spark):
    return st.clean_products(spark.createDataFrame(
        [("P1", "Technology", "Phones", "1.0"), ("P2", "Furniture", "Chairs", "2.0")],
        "product_id string, category string, sub_category string, price_per_product string"))


# ---------------------------------------------------------------- 1. Read and save
def test_standardize_columns(spark):
    df = spark.createDataFrame([(1, 2, 3)], "`Customer ID` int, `Sub-Category` int, `Price per product` int")
    assert st.standardize_columns(df).columns == ["customer_id", "sub_category", "price_per_product"]


@pytest.mark.parametrize("raw, expected", [
    (" Order  ID ", "order_id"),         # extra spaces
    ("Product Name (EN)", "product_name_en"),  # brackets
    ("already_clean", "already_clean"),  # nothing to change
])
def test_standardize_columns_edge_cases(spark, raw, expected):
    df = spark.createDataFrame([(1,)], "a int").toDF(raw)
    assert st.standardize_columns(df).columns == [expected]


def test_save_table_without_schema_creates_temp_view(spark):
    name = st.save_table(spark.createDataFrame([(1,)], "a int"), "tmp_save_table_test")
    assert spark.table(name).count() == 1


def test_save_table_overwrites_old_data(spark):
    """Re-running the pipeline must replace data, not append to it."""
    st.save_table(spark.createDataFrame([(1,)], "a int"), "tmp_save_overwrite_test")
    name = st.save_table(spark.createDataFrame([(1,), (2,), (3,)], "a int"), "tmp_save_overwrite_test")
    assert spark.table(name).count() == 3


def test_read_orders_missing_file_fails(spark):
    """Negative: a missing source file must fail loudly, not load zero rows silently."""
    with pytest.raises(Exception):
        st.read_orders(spark, f"file:{DATA}/does_not_exist.json").collect()


# ---------------------------------------------------------------- 2. Clean
@pytest.mark.parametrize("raw, expected", [
    ("Gary567 Hansen", "Gary Hansen"),            # digits removed
    ("Pete@#$ Takahito", "Pete Takahito"),        # symbols removed
    ("  Tracy   Blumstein ", "Tracy Blumstein"),   # extra spaces removed
    ("Jack O'Briant", "Jack O'Briant"),           # real apostrophe kept
    ("Peter Bühler", "Peter Bühler"),             # accented letters kept
    ("12345", None),                              # nothing left -> NULL
    ("", None),                                   # empty string -> NULL
    ("   ", None),                                # only spaces -> NULL
    (None, None),
])
def test_clean_customer_name(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "name string")
    assert df.select(st.clean_customer_name(F.col("name"))).first()[0] == expected


def test_clean_customers(spark):
    raw = spark.createDataFrame(
        [(" c1 ", "Alex Avila", "United States"),
         ("C1", "Alex Avila", "United States"),      # duplicate id
         ("C2", None, None),                          # missing name and country
         (None, "No Id", "United States")],           # no id -> dropped
        "customer_id string, customer_name string, country string")
    rows = {r.customer_id: r for r in st.clean_customers(raw).collect()}
    assert set(rows) == {"C1", "C2"}
    assert (rows["C2"].customer_name, rows["C2"].country) == ("Unknown", "Unknown")


def test_clean_customers_blank_values(spark):
    """Negative: blank id is dropped; blank name/country become 'Unknown'."""
    raw = spark.createDataFrame(
        [("   ", "Blank Id", "United States"),       # blank id -> dropped
         ("C3", "  ", "  ")],                         # blank name and country
        "customer_id string, customer_name string, country string")
    rows = st.clean_customers(raw).collect()
    assert [(r.customer_id, r.customer_name, r.country) for r in rows] == [("C3", "Unknown", "Unknown")]


def test_clean_customers_empty_input(spark):
    """Negative: an empty file gives an empty result, not an error."""
    raw = spark.createDataFrame([], "customer_id string, customer_name string, country string")
    assert st.clean_customers(raw).count() == 0


def test_clean_products_keeps_one_row_per_product(spark):
    raw = spark.createDataFrame(
        [("P1", "Technology", "Phones", "1.5"),
         ("P1", "Technology", "Phones", "2.5"),      # duplicate id (as in Products.csv)
         ("P2", "Furniture", None, "bad")],           # missing sub-category, bad price
        "product_id string, category string, sub_category string, price_per_product string")
    rows = {r.product_id: r for r in st.clean_products(raw).collect()}
    assert set(rows) == {"P1", "P2"}
    assert rows["P2"].sub_category == "Unknown"
    assert rows["P2"].price_per_product is None


def test_clean_products_blank_values(spark):
    """Negative: blank id is dropped; blank category/sub-category become 'Unknown'."""
    raw = spark.createDataFrame(
        [("", "Technology", "Phones", "1.0"),         # blank id -> dropped
         ("P3", "  ", "", "")],                       # blank category, sub-category, price
        "product_id string, category string, sub_category string, price_per_product string")
    rows = st.clean_products(raw).collect()
    assert [(r.product_id, r.category, r.sub_category, r.price_per_product) for r in rows] == [
        ("P3", "Unknown", "Unknown", None)]


def test_clean_orders_parses_day_first_dates(spark):
    row = st.clean_orders(spark.createDataFrame([order(order_date="2/7/2015")], ORDER_SCHEMA)).first()
    assert row.order_date == dt.date(2015, 7, 2)     # 2 July, not 7 February
    assert row.profit == 10.0 and row.quantity == 2


@pytest.mark.parametrize("raw, expected", [
    ("29/2/2016", dt.date(2016, 2, 29)),     # leap day
    (" 21/8/2016 ", dt.date(2016, 8, 21)),   # extra spaces
    ("01/12/2016", dt.date(2016, 12, 1)),    # leading zero
])
def test_clean_orders_date_edge_cases(spark, raw, expected):
    row = st.clean_orders(spark.createDataFrame([order(order_date=raw)], ORDER_SCHEMA)).first()
    assert row.order_date == expected


# Negative: every row below is missing something required, so it must be dropped.
@pytest.mark.parametrize("bad_order", [
    pytest.param(order(order_date="31/2/2016"), id="impossible date"),
    pytest.param(order(order_date="29/2/2017"), id="not a leap year"),
    pytest.param(order(order_date="21/13/2016"), id="month 13"),
    pytest.param(order(order_date="2016-08-21"), id="wrong date format"),
    pytest.param(order(order_date="21/8/16"), id="2-digit year"),
    pytest.param(order(order_date=""), id="empty date"),
    pytest.param(order(order_date=None), id="missing date"),
    pytest.param(order(order_id=None), id="missing order id"),
    pytest.param(order(order_id="  "), id="blank order id"),
    pytest.param(order(customer_id=None), id="missing customer id"),
    pytest.param(order(customer_id="   "), id="blank customer id"),
    pytest.param(order(product_id=None), id="missing product id"),
    pytest.param(order(product_id=""), id="blank product id"),
    pytest.param(order(profit="n/a"), id="text profit"),
    pytest.param(order(profit=""), id="empty profit"),
    pytest.param(order(profit=None), id="missing profit"),
])
def test_clean_orders_drops_invalid_rows(spark, bad_order):
    df = spark.createDataFrame([bad_order, order(row_id="2")], ORDER_SCHEMA)
    assert [r.row_id for r in st.clean_orders(df).collect()] == ["2"]


def test_clean_orders_removes_duplicates(spark):
    df = spark.createDataFrame([order(), order(), order(row_id="2")], ORDER_SCHEMA)
    assert st.clean_orders(df).count() == 2


def test_clean_orders_same_row_id_counted_once(spark):
    """Negative: same row_id sent twice with different values must not double the profit."""
    df = spark.createDataFrame([order(profit="10"), order(profit="99")], ORDER_SCHEMA)
    assert st.clean_orders(df).count() == 1


def test_clean_orders_keeps_row_with_bad_optional_fields(spark):
    """Negative: bad ship date / quantity are not required, so the order is kept with NULLs."""
    df = spark.createDataFrame([order(ship_date="not a date", quantity="abc")], ORDER_SCHEMA)
    rows = st.clean_orders(df).collect()
    assert len(rows) == 1
    assert (rows[0].ship_date, rows[0].quantity) == (None, None)


def test_clean_orders_all_rows_invalid_gives_empty_result(spark):
    df = spark.createDataFrame([order(profit=None), order(row_id="2", customer_id=None)], ORDER_SCHEMA)
    assert st.clean_orders(df).count() == 0


def test_clean_orders_missing_required_column_fails(spark):
    """Negative: if the source file loses a column (schema change), the job must fail loudly."""
    df = spark.createDataFrame([order()], ORDER_SCHEMA).drop("profit")
    with pytest.raises(Exception):
        st.clean_orders(df).collect()


# ---------------------------------------------------------------- 2b. Reject reasons (data quality)
@pytest.mark.parametrize("bad_order, reason", [
    pytest.param(order(order_id="  "), "missing order id", id="blank order id"),
    pytest.param(order(customer_id=None), "missing customer id", id="missing customer id"),
    pytest.param(order(product_id=""), "missing product id", id="blank product id"),
    pytest.param(order(order_date="31/2/2016"), "invalid order date", id="impossible date"),
    pytest.param(order(profit="n/a"), "invalid profit", id="text profit"),
])
def test_add_reject_reason(spark, bad_order, reason):
    df = spark.createDataFrame([bad_order], ORDER_SCHEMA)
    assert st.add_reject_reason(df).first().reject_reason == reason


def test_add_reject_reason_valid_row_has_no_reason(spark):
    df = spark.createDataFrame([order()], ORDER_SCHEMA)
    assert st.add_reject_reason(df).first().reject_reason is None


def test_add_reject_reason_first_problem_is_reported(spark):
    """Row with several problems: the first rule in the list (customer id) is reported."""
    df = spark.createDataFrame([order(customer_id=None, order_date="bad", profit=None)], ORDER_SCHEMA)
    assert st.add_reject_reason(df).first().reject_reason == "missing customer id"


def test_reject_reason_matches_clean_orders(spark):
    """Rows with no reason are exactly the rows clean_orders keeps (no row lost or double counted)."""
    df = spark.createDataFrame([order(row_id="1"), order(row_id="2", profit="bad"),
                                order(row_id="3", customer_id=" "), order(row_id="4")], ORDER_SCHEMA)
    valid = st.add_reject_reason(df).filter(F.col("reject_reason").isNull())
    assert sorted(r.row_id for r in valid.collect()) == sorted(r.row_id for r in st.clean_orders(df).collect())


# ---------------------------------------------------------------- 3. Enrich
@pytest.mark.parametrize("profit, expected", [("10.5678", 10.57), ("-14.9249", -14.92), ("2.004", 2.0)])
def test_enriched_orders_rounds_profit_to_2dp(spark, customers, products, profit, expected):
    orders = st.clean_orders(spark.createDataFrame([order(profit=profit)], ORDER_SCHEMA))
    assert st.build_enriched_orders(orders, customers, products).first().profit == expected


def test_enriched_orders_adds_customer_and_product_columns(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([order()], ORDER_SCHEMA))
    row = st.build_enriched_orders(orders, customers, products).first()
    assert (row.customer_name, row.country) == ("Alex Avila", "United States")
    assert (row.category, row.sub_category) == ("Technology", "Phones")
    assert row.order_year == 2016


def test_enriched_orders_keeps_orders_without_match(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([order(customer_id="C9", product_id="P9")], ORDER_SCHEMA))
    row = st.build_enriched_orders(orders, customers, products).first()
    assert (row.customer_name, row.country, row.category, row.sub_category) == ("Unknown",) * 4


def test_enriched_orders_row_count_unchanged(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([order(row_id=str(i)) for i in range(5)], ORDER_SCHEMA))
    assert st.build_enriched_orders(orders, customers, products).count() == 5


def test_enriched_orders_matches_ids_with_case_and_spaces(spark, customers, products):
    """' c1 ' and 'p1 ' in orders must still match customer C1 and product P1."""
    orders = st.clean_orders(spark.createDataFrame([order(customer_id=" c1 ", product_id="p1 ")], ORDER_SCHEMA))
    row = st.build_enriched_orders(orders, customers, products).first()
    assert (row.customer_name, row.category) == ("Alex Avila", "Technology")


def test_enriched_orders_partial_match(spark, customers, products):
    """Negative: customer found but product not found -> only product columns are 'Unknown'."""
    orders = st.clean_orders(spark.createDataFrame([order(customer_id="C1", product_id="P9")], ORDER_SCHEMA))
    row = st.build_enriched_orders(orders, customers, products).first()
    assert (row.customer_name, row.category, row.sub_category) == ("Alex Avila", "Unknown", "Unknown")


def test_enriched_orders_duplicate_master_rows_do_not_duplicate_orders(spark):
    """Negative: repeated ids in Customer/Product files must not multiply order lines."""
    customers = st.clean_customers(spark.createDataFrame(
        [("C1", "Alex Avila", "United States"), ("C1", "Alex Avila", "United States")],
        "customer_id string, customer_name string, country string"))
    products = st.clean_products(spark.createDataFrame(
        [("P1", "Technology", "Phones", "1.0"), ("P1", "Technology", "Phones", "2.0")],
        "product_id string, category string, sub_category string, price_per_product string"))
    orders = st.clean_orders(spark.createDataFrame([order()], ORDER_SCHEMA))
    assert st.build_enriched_orders(orders, customers, products).count() == 1


def test_enriched_orders_empty_orders(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([], ORDER_SCHEMA))
    assert st.build_enriched_orders(orders, customers, products).count() == 0


# ---------------------------------------------------------------- 4. Aggregate
@pytest.fixture
def agg(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([
        order(row_id="1", order_date="1/1/2016", customer_id="C1", product_id="P1", profit="10.00"),
        order(row_id="2", order_date="9/3/2016", customer_id="C1", product_id="P1", profit="5.00"),
        order(row_id="3", order_date="5/6/2016", customer_id="C1", product_id="P2", profit="-20.00"),
        order(row_id="4", order_date="3/3/2017", customer_id="C2", product_id="P1", profit="7.50"),
    ], ORDER_SCHEMA))
    return st.build_profit_aggregate(st.build_enriched_orders(orders, customers, products))


def test_profit_aggregate(agg):
    rows = {(r.order_year, r.category, r.sub_category, r.customer_name): r.total_profit for r in agg.collect()}
    assert rows == {
        (2016, "Technology", "Phones", "Alex Avila"): 15.0,
        (2016, "Furniture", "Chairs", "Alex Avila"): -20.0,
        (2017, "Technology", "Phones", "Ben Wallace"): 7.5,
    }


def test_profit_by_year(agg):
    assert [tuple(r) for r in st.profit_by(agg, "order_year").collect()] == [(2016, -5.0), (2017, 7.5)]


def test_profit_by_year_and_category(agg):
    assert [tuple(r) for r in st.profit_by(agg, "order_year", "category").collect()] == [
        (2016, "Furniture", -20.0), (2016, "Technology", 15.0), (2017, "Technology", 7.5)]


def test_profit_by_customer(agg):
    assert [tuple(r) for r in st.profit_by(agg, "customer_id", "customer_name").collect()] == [
        ("C1", "Alex Avila", -5.0), ("C2", "Ben Wallace", 7.5)]


def test_profit_by_customer_and_year(agg):
    assert [tuple(r) for r in st.profit_by(agg, "customer_id", "customer_name", "order_year").collect()] == [
        ("C1", "Alex Avila", 2016, -5.0), ("C2", "Ben Wallace", 2017, 7.5)]


def test_profit_aggregate_groups_unmatched_as_unknown(spark, customers, products):
    """Negative: orders with unknown products are reported under 'Unknown', not lost."""
    orders = st.clean_orders(spark.createDataFrame([
        order(row_id="1", product_id="P8", profit="1.00"),
        order(row_id="2", product_id="P9", profit="2.00"),
    ], ORDER_SCHEMA))
    agg = st.build_profit_aggregate(st.build_enriched_orders(orders, customers, products))
    assert [(r.category, r.sub_category, r.total_profit) for r in agg.collect()] == [("Unknown", "Unknown", 3.0)]


def test_profit_aggregate_no_floating_point_noise(spark, customers, products):
    """0.10 + 0.20 must show as 0.3, not 0.30000000000000004."""
    orders = st.clean_orders(spark.createDataFrame([
        order(row_id="1", profit="0.10"), order(row_id="2", profit="0.20")], ORDER_SCHEMA))
    agg = st.build_profit_aggregate(st.build_enriched_orders(orders, customers, products))
    assert agg.first().total_profit == 0.3


def test_profit_aggregate_empty_input(spark, customers, products):
    orders = st.clean_orders(spark.createDataFrame([], ORDER_SCHEMA))
    assert st.build_profit_aggregate(st.build_enriched_orders(orders, customers, products)).count() == 0


def test_profit_by_unknown_column_fails(agg):
    """Negative: asking for a column that does not exist must raise an error."""
    with pytest.raises(Exception):
        st.profit_by(agg, "region").collect()


# ---------------------------------------------------------------- End to end on the sample files


@pytest.mark.skipif(not os.path.exists(os.path.join(DATA, "Orders.json")), reason="sample data not found")
def test_pipeline_on_sample_files(spark):
    """No order is lost by the joins and total profit is the same at every step."""
    base = f"file:{DATA}"
    orders = st.clean_orders(st.standardize_columns(st.read_orders(spark, f"{base}/Orders.json")))
    customers = st.clean_customers(st.standardize_columns(st.read_customers(spark, f"{base}/Customer.xlsx")))
    products = st.clean_products(st.standardize_columns(st.read_products(spark, f"{base}/Products.csv")))
    enriched = st.build_enriched_orders(orders, customers, products)
    agg = st.build_profit_aggregate(enriched)

    assert enriched.count() == orders.count()
    # no NULL keys reach the final tables
    missing_keys = enriched.filter(F.col("order_id").isNull() | F.col("customer_id").isNull()
                                   | F.col("product_id").isNull() | F.col("order_year").isNull())
    assert missing_keys.count() == 0
    total = enriched.agg(F.sum("profit")).first()[0]
    assert agg.agg(F.sum("total_profit")).first()[0] == pytest.approx(total, abs=0.01)
    assert st.profit_by(agg, "order_year").agg(F.sum("total_profit")).first()[0] == pytest.approx(total, abs=0.01)
