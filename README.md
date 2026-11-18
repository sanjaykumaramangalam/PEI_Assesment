# Sales Data Transformation (PySpark)

## Layout
```
Test_Data/
  data/                         Orders.json, Products.csv, Customer.xlsx (sources)
  src/sales_transforms.py       all transformation logic (pure PySpark functions)
  tests/conftest.py             pytest fixtures (Spark session, row builders)
  tests/test_sales_transforms.py  unit + integration tests (58 cases)
  notebooks/01_sales_pipeline   runs the pipeline end to end
  notebooks/02_run_unit_tests   runs pytest; fails if any test fails
```

## How to run
1. Run `notebooks/02_run_unit_tests`. It should report all tests passed.
2. Run `notebooks/01_sales_pipeline`.
   * `target_schema` blank: outputs are session temp views (no write permission needed).
   * `target_schema = catalog.schema`: outputs are managed Delta tables (overwrite, so re-runs are idempotent).

## Outputs
| Table | Grain | Notes |
| --- | --- | --- |
| raw_orders / raw_customers / raw_products | source row | all columns STRING, snake_case names, `_source`, `_ingested_at` |
| enriched_customers | customer_id | cleaned name (+ `customer_name_raw`, `is_name_missing`), ZIP leading zeros restored |
| enriched_products | product_id | de-duplicated; `source_row_count` shows source duplicates |
| orders_rejected | order line | rows failing rules, with `dq_errors` array |
| enriched_orders | order line | order info, `profit` DECIMAL(18,2) rounded half-up, customer name/country, category/sub-category, match flags |
| agg_profit | year x category x sub-category x customer | `total_profit`, `order_count`, `order_line_count` |

SQL aggregates (task 5) are defined in `SQL_AGGREGATES` and run with `spark.sql` on `agg_profit`: profit by year, by year + category, by customer, and by customer + year.

## Data-quality findings (sample data)
* Orders: 9,994 rows; dates are day-first (`21/8/2016`); profit ranges from negative to positive.
* 204 order lines reference a Product ID not present in Products.csv. They are kept and labelled `Unknown` so profit still reconciles.
* Products.csv: 1,851 rows but only 1,818 distinct Product IDs. Category and sub-category are consistent per ID; name, state and price differ.
* Customer.xlsx: 793 customers; 8 null names; dozens of names with injected digits or symbols (e.g. `Gary567 Hansen`, `Tam&^*ara Willing___)ham`).

## Assumptions and decisions
* Raw = lossless strings; typing happens in the enriched layer using `try_cast` / `try_to_timestamp` so bad values become NULL and are quarantined instead of failing the job.
* Order rejection rules: malformed record, missing row/order/customer/product id, invalid order date, quantity NULL or <= 0, invalid profit, duplicate row_id.
* Money is DECIMAL, not DOUBLE. Profit is rounded to 2 dp per order line, then summed.
* Duplicate Product IDs: one row is kept deterministically (first by product_name, then state). Only category and sub-category are used downstream, and those agree.
* Customers with a missing name are shown as `Unknown` (`is_name_missing = true`).
* "Enriched table for customers and products" is implemented as two tables, one per dimension.
* The aggregate is kept at the finest requested grain, so every SQL roll-up is an exact SUM of it.

## Known limitations of name cleaning
Without a reference list of names, some noise cannot be undone: `Bi1l` becomes `Bil`, `N0ra` becomes `Nra`, `Maribeth 5chnelling` becomes `Maribethchnelling`, and `Craig M 876olinari` becomes `Craig M olinari`. The original value is kept in `customer_name_raw`.

## Performance notes
* Explicit JSON schema avoids a schema-inference pass over Orders.json.
* Dimensions are small, so they are broadcast to the order join.
* Uniqueness is asserted on dimension keys before the join to prevent fan-out.
* No Python UDFs; all logic is native Spark expressions.
