"""
Sales assessment: PySpark transformations
=========================================
Pipeline: read files -> save raw tables -> clean -> enrich -> aggregate.

Every function takes DataFrame(s) and returns a DataFrame, so each step is unit
tested with a small in-memory DataFrame (see tests/test_sales_transforms.py).

Assumptions
* Order Date / Ship Date are day-first strings, e.g. "21/8/2016".
* Products.csv repeats some Product IDs (same category); keep one row per ID so
  the join does not duplicate order lines.
* Orders with no matching customer or product are kept (left join) and labelled
  "Unknown", so no profit is lost.
* Orders missing an ID, a valid order date or profit are dropped (count is reported).
* Blank or whitespace-only text ("", "   ") is treated the same as missing (NULL).
"""
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATE_FORMAT = "d/M/yyyy"
UNKNOWN = "Unknown"


# ---------------------------------------------------------------- 1. Read and save
def read_orders(spark: SparkSession, path: str) -> DataFrame:
    """Orders.json is one JSON array spread over many lines."""
    return spark.read.option("multiLine", True).json(path)


def read_products(spark: SparkSession, path: str) -> DataFrame:
    """Product names contain commas and escaped quotes ("")."""
    return (spark.read.option("header", True).option("escape", '"')
            .option("multiLine", True).csv(path))


def read_customers(spark: SparkSession, path: str) -> DataFrame:
    """Customer.xlsx: first sheet, first row is the header."""
    return spark.read.format("excel").option("headerRows", 1).load(path)


def standardize_columns(df: DataFrame) -> DataFrame:
    """'Customer ID' -> 'customer_id', 'Sub-Category' -> 'sub_category' (Delta-safe names)."""
    return df.toDF(*[re.sub(r"[^0-9a-zA-Z]+", "_", c.strip()).strip("_").lower() for c in df.columns])


def save_table(df: DataFrame, name: str, schema: str = None) -> str:
    """Overwrite Delta table <schema>.<name>; with no schema, register a temp view instead."""
    if schema:
        full_name = f"{schema}.{name}"
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
        return full_name
    df.createOrReplaceTempView(name)
    return name


# ---------------------------------------------------------------- 2. Clean
def blank_to_null(col_name: str):
    """Trim text; empty or whitespace-only values become NULL."""
    value = F.trim(F.col(col_name))
    return F.when(value != "", value)


def clean_id(col_name: str):
    """' c1 ' -> 'C1'; blank id -> NULL so the row is treated as missing an id."""
    return F.upper(blank_to_null(col_name))


def clean_customer_name(name):
    """Strip digits/symbols injected into names: 'Gary567 Hansen' -> 'Gary Hansen'."""
    name = F.regexp_replace(name, r"[^\p{L}' ]", "")       # keep letters, apostrophe, space
    name = F.trim(F.regexp_replace(name, r"\s+", " "))      # collapse repeated spaces
    return F.when(name != "", name)                         # empty -> NULL


def clean_customers(df: DataFrame) -> DataFrame:
    """One row per customer_id; cleaned name; missing name/country -> 'Unknown'."""
    return (
        df.withColumn("customer_id", clean_id("customer_id"))
        .withColumn("customer_name", F.coalesce(clean_customer_name(F.col("customer_name")), F.lit(UNKNOWN)))
        .withColumn("country", F.coalesce(blank_to_null("country"), F.lit(UNKNOWN)))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def clean_products(df: DataFrame) -> DataFrame:
    """One row per product_id (the source file repeats some IDs)."""
    return (
        df.withColumn("product_id", clean_id("product_id"))
        .withColumn("category", F.coalesce(blank_to_null("category"), F.lit(UNKNOWN)))
        .withColumn("sub_category", F.coalesce(blank_to_null("sub_category"), F.lit(UNKNOWN)))
        .withColumn("price_per_product", F.expr("try_cast(price_per_product AS DOUBLE)"))
        .filter(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )


def to_date_dmy(col_name: str):
    """'21/8/2016' -> 2016-08-21. Invalid dates become NULL instead of failing the job."""
    return F.to_date(F.try_to_timestamp(F.trim(F.col(col_name).cast("string")), F.lit(DATE_FORMAT)))


def clean_orders(df: DataFrame) -> DataFrame:
    """Typed order lines. Drops rows missing an ID, a valid order date or profit, and duplicate rows."""
    return (
        df.withColumn("order_date", to_date_dmy("order_date"))
        .withColumn("ship_date", to_date_dmy("ship_date"))
        .withColumn("order_id", blank_to_null("order_id"))
        .withColumn("customer_id", clean_id("customer_id"))
        .withColumn("product_id", clean_id("product_id"))
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("price", F.expr("try_cast(price AS DOUBLE)"))
        .withColumn("discount", F.expr("try_cast(discount AS DOUBLE)"))
        .withColumn("profit", F.expr("try_cast(profit AS DOUBLE)"))
        .dropna(subset=["order_id", "customer_id", "product_id", "order_date", "profit"])
        .dropDuplicates(["row_id"])
    )


def add_reject_reason(raw_orders: DataFrame) -> DataFrame:
    """Add 'reject_reason' to raw orders: why clean_orders would drop the row (NULL = row is valid).

    Uses the same rules as clean_orders, so bad rows can be saved to a rejected table with a reason.
    If a row has several problems, the first one in this list is reported.
    """
    reason = (
        F.when(blank_to_null("order_id").isNull(), "missing order id")
        .when(clean_id("customer_id").isNull(), "missing customer id")
        .when(clean_id("product_id").isNull(), "missing product id")
        .when(to_date_dmy("order_date").isNull(), "invalid order date")
        .when(F.expr("try_cast(profit AS DOUBLE)").isNull(), "invalid profit")
    )
    return raw_orders.withColumn("reject_reason", reason)


# ---------------------------------------------------------------- 3. Enrich
def build_enriched_orders(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Order info + profit rounded to 2 dp + customer name/country + product category/sub-category."""
    customers = customers.select("customer_id", "customer_name", "country")
    products = products.select("product_id", "category", "sub_category")
    return (
        orders.join(F.broadcast(customers), "customer_id", "left")    # small tables -> broadcast join
        .join(F.broadcast(products), "product_id", "left")
        .withColumn("profit", F.round("profit", 2))
        .withColumn("order_year", F.year("order_date"))
        .fillna(UNKNOWN, subset=["customer_name", "country", "category", "sub_category"])
        .select("row_id", "order_id", "order_date", "order_year", "ship_date", "ship_mode",
                "quantity", "price", "discount", "profit",
                "customer_id", "customer_name", "country",
                "product_id", "category", "sub_category")
    )


# ---------------------------------------------------------------- 4. Aggregate
def build_profit_aggregate(enriched_orders: DataFrame) -> DataFrame:
    """Profit by Year, Product Category, Product Sub Category and Customer."""
    return (
        enriched_orders.groupBy("order_year", "category", "sub_category", "customer_id", "customer_name")
        .agg(F.round(F.sum("profit"), 2).alias("total_profit"))
    )


def profit_by(agg: DataFrame, *keys: str) -> DataFrame:
    """Roll the aggregate table up to any of its keys, e.g. profit_by(agg, "order_year")."""
    return (
        agg.groupBy(*keys)
        .agg(F.round(F.sum("total_profit"), 2).alias("total_profit"))
        .orderBy(*keys)
    )
