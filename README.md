# Sales Assessment Pipeline

PySpark pipeline that loads Products, Customers and Orders, cleans and enriches them, and
reports profit by year, product category, sub-category and customer.

## Layout

```
Assesment/
  data/                    copies of the source files (pipeline reads from the UC volume)
  src/
    lib/sales_lib.py       all business logic (readers, cleaning, joins, checks, aggregates)
    ingestion              1. raw (bronze) layer
    transformations        2. enriched (silver) layer
    aggregations           3. aggregate (gold) layer
  SQL_agg/SQL_Aggeration   4. SQL profit views
  tests/
    test_sales_lib.py      pytest suite (36 tests)
    run_tests              notebook that runs the suite
```

Notebooks only orchestrate (read -> call `sales_lib` -> check -> write). Every rule lives in
`sales_lib.py`, so it is tested once and reused.

## How to run

1. `tests/run_tests` (fails if any test fails)
2. `src/ingestion` -> `src/transformations` -> `src/aggregations`
3. `SQL_agg/SQL_Aggeration`

Source: `/Volumes/test/db_name/aag_files/Learning_Files/`. Target schema: `test.db_name`.
Runs on serverless compute.

## Tables

| Layer | Table | Rows | Grain |
| --- | --- | --- | --- |
| Raw | `raw_product` | 1,851 | file row |
| Raw | `raw_customer` | 793 | file row |
| Raw | `raw_order` | 9,994 | order line |
| Enriched | `enriched_customer` | 793 | `customer_id` |
| Enriched | `enriched_product` | 1,818 | `product_id` |
| Enriched | `enriched_order` | 9,994 | `row_id` (order line) |
| Aggregate | `agg_profit_by_year_category_customer` | 8,129 | year, category, sub_category, customer |

Raw tables add `_source_file` and `_ingest_ts`; enriched and aggregate tables add `_processed_ts`.

## Assumptions

* Source files are full snapshots, so each layer is fully overwritten. Re-running is safe (idempotent).
* Order and ship dates are day-first strings (`d/M/yyyy`, e.g. `3/1/2017` = 3 Jan 2017).
  An unparseable date becomes NULL and fails a blocking check.
* Profit is rounded to 2 decimals in `enriched_order`. The aggregate sums the rounded values,
  then rounds again.
* `order_count` counts distinct `order_id` (one order can have several lines).
* `raw_product` has 33 extra rows for duplicate `product_id`s. Category and sub-category
  are identical within a `product_id`, so one row is kept (alphabetically first `product_name`,
  then `state`). Profit by category is therefore not affected.
* 204 order lines reference a product missing from `Products.csv`. They are kept (profit must
  not be lost), with category/sub-category `Unknown` and `product_found = false`.
* Customer names contain injected noise. `clean_person_name` repairs it:
  look-alike characters are mapped (`Bi1l` -> `Bill`, `Wa55erman` -> `Wasserman`, `C@thy` -> `Cathy`),
  junk is removed (`Tam&^*ara` -> `Tamara`), split words are rejoined (`Anth 34789ony` -> `Anthony`),
  and `'` / `-` / accented letters are kept. Names with no letters
  (8 customers) become `Unknown` with `is_name_missing = true`.
* Email is validated and flagged (`is_valid_email`), not blocked. Phone is not used in any output,
  so it is kept as in the source.
* `customer_name` in the aggregate comes from `enriched_customer` (the master), not from the order file.

## Data quality and error handling

| Stage | Control | On failure |
| --- | --- | --- |
| Read | File exists and is not empty (`check_source_file`) | `FileNotFoundError` / `ValueError` |
| Read | Explicit schemas (CSV, JSON); required columns check (Excel) | `ValueError` naming missing columns |
| Read | PERMISSIVE mode, bad rows kept in `_corrupt_record` | Row filtered out in silver |
| Enriched | Unknown product/customer, invalid email, cleaned names | Flag columns (non-blocking) |

## Tests

`tests/test_sales_lib.py` uses small hand-built DataFrames with known answers:

* Name cleaning: 16 noisy/clean cases + 5 "no letters" cases
* Helpers: snake_case columns, required columns, missing/empty files, corrupt rows dropped
* Customers: name repair, `Unknown`, email flag, dedup, blank ids dropped, missing column
* Products: dedup choice, NULL -> `Unknown`, price rounding
* Orders: joins, `Unknown` for missing master data, day-first dates, invalid date -> NULL,
  profit rounding, duplicates in a dimension do not multiply order rows
* Aggregates: year/customer grouping, distinct order count, total reconciles
* Integration: each reader loads the expected row count from the real files

## Performance

* Explicit schemas: no inference pass over the files.
* Small dimensions (customers, products) are broadcast in the order join, so there is no shuffle of the order data.
* Dimensions are made unique before the join, so the join cannot multiply rows.
* Everything, including name cleaning, uses built-in Spark functions (no UDFs).
* For much larger or incremental data: switch the overwrite to `MERGE` / Auto Loader, and
  partition or liquid-cluster `enriched_order` by order date.

## Known limitations

* A single space inside a name cannot be told apart from a real word break (`Kat rina` -> `Kat Rina`).
* A look-alike character inside a longer junk run is treated as noise (`Bi 8761l` -> `Bil`).
