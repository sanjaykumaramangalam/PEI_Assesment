# Sales Pipeline - Test Case Document

Project: `/Workspace/Users/8621028@ups.com/Test_Data`
Last test run: 2026-11-18, serverless, **74 passed, 0 failed**

This document lists every test case, where the **test code** is, and where the **actual code being tested** is.
Line numbers are from the files as of 2026-11-18.

---

## 1. Where everything lives

| Folder | File | What it contains |
| --- | --- | --- |
| `Test_Data/src` | `sales_transforms.py` | **Actual code** - all read, clean, enrich and aggregate functions |
| `Test_Data/tests` | `test_sales_transforms.py` | **Unit test cases** (74 after expanding parametrized cases) |
| `Test_Data/tests` | `conftest.py` | Test setup only (Spark session, path to `src`) - no test cases |
| `Test_Data/notebooks` | `01_sales_pipeline` | Runs the pipeline: raw -> enriched -> aggregate tables |
| `Test_Data/notebooks` | `02_run_unit_tests` | Runs all unit tests with pytest; fails if any test fails |
| `Test_Data/notebooks` | `03_data_quality_checks` | Applies the same rules to the **real tables** (Critical / Reject / Warn) |
| `Test_Data/data` | `Orders.json`, `Customer.xlsx`, `Products.csv` | Sample source files (used by the end-to-end test) |
| `Test_Data` | `test_case_coverage.csv` | Same test cases in Excel format |

## 2. How to run

* **Unit tests:** open `notebooks/02_run_unit_tests` and run all cells. The last cell fails if any test fails.
* **Data quality on real tables:** run `notebooks/03_data_quality_checks` after `01_sales_pipeline`.
* **Job order:** `02_run_unit_tests` -> `01_sales_pipeline` -> `03_data_quality_checks`.

## 3. Actual code - function index (`src/sales_transforms.py`)

| Function | Lines | Purpose |
| --- | --- | --- |
| `read_orders` | 28-30 | Read Orders.json (multi-line JSON) |
| `read_products` | 33-36 | Read Products.csv (quoted names with commas) |
| `read_customers` | 39-41 | Read Customer.xlsx |
| `standardize_columns` | 44-46 | `Customer ID` -> `customer_id` |
| `save_table` | 49-56 | Overwrite Delta table, or temp view when no schema |
| `blank_to_null` | 60-63 | Trim text; blank -> NULL |
| `clean_id` | 66-68 | Trim + upper-case id; blank -> NULL |
| `clean_customer_name` | 71-75 | Remove digits/symbols from names |
| `clean_customers` | 78-86 | One row per customer, defaults to `Unknown` |
| `clean_products` | 89-98 | One row per product, defaults to `Unknown` |
| `to_date_dmy` | 101-103 | Parse day-first dates; invalid -> NULL |
| `clean_orders` | 106-120 | Type columns, drop invalid rows and duplicates |
| `add_reject_reason` | 123-136 | Give the reason an order is rejected (for `rejected_orders`) |
| `build_enriched_orders` | 140-154 | Join orders with customers and products, round profit |
| `build_profit_aggregate` | 158-163 | Profit by year, category, sub-category, customer |
| `profit_by` | 166-172 | Roll the aggregate up to any key |

## 4. Test cases

Test file: `Test_Data/tests/test_sales_transforms.py`
Code file: `Test_Data/src/sales_transforms.py`
Type: **P** = Positive, **N** = Negative

### 4.1 Read and save

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC01 | Column names standardised (`Customer ID` -> `customer_id`) | P | `test_standardize_columns` (45-47) | `standardize_columns` (44-46) |
| TC02 | Column name with extra spaces | P | `test_standardize_columns_edge_cases` (50-57) | `standardize_columns` (44-46) |
| TC03 | Column name with brackets | P | `test_standardize_columns_edge_cases` (50-57) | `standardize_columns` (44-46) |
| TC04 | Column name already clean | P | `test_standardize_columns_edge_cases` (50-57) | `standardize_columns` (44-46) |
| TC05 | Save without schema creates temp view | P | `test_save_table_without_schema_creates_temp_view` (60-62) | `save_table` (49-56) |
| TC06 | Re-run replaces old data (no append) | P | `test_save_table_overwrites_old_data` (65-69) | `save_table` (49-56) |
| TC07 | Source file missing -> job fails | N | `test_read_orders_missing_file_fails` (72-75) | `read_orders` (28-30) |

### 4.2 Clean - customers and products

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC08 | Digits removed from name (`Gary567 Hansen`) | P | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC09 | Symbols removed from name (`Pete@#$ Takahito`) | P | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC10 | Extra spaces removed from name | P | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC11 | Apostrophe kept (`O'Briant`) | P | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC12 | Accented letters kept (`Bühler`) | P | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC13 | Name with only digits -> NULL | N | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC14 | Empty name -> NULL | N | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC15 | Name with only spaces -> NULL | N | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC16 | Missing name (NULL) -> NULL | N | `test_clean_customer_name` (79-92) | `clean_customer_name` (71-75) |
| TC17 | Customer id trimmed/upper-cased, duplicates removed, NULL id dropped, missing name/country -> `Unknown` | P/N | `test_clean_customers` (95-104) | `clean_customers` (78-86), `clean_id` (66-68) |
| TC18 | Blank customer id dropped; blank name/country -> `Unknown` | N | `test_clean_customers_blank_values` (107-114) | `clean_customers` (78-86), `blank_to_null` (60-63) |
| TC19 | Empty customer file -> 0 rows, no error | N | `test_clean_customers_empty_input` (117-120) | `clean_customers` (78-86) |
| TC20 | Duplicate product id -> one row; missing sub-category -> `Unknown`; bad price -> NULL | P/N | `test_clean_products_keeps_one_row_per_product` (123-132) | `clean_products` (89-98) |
| TC21 | Blank product id dropped; blank category/price handled | N | `test_clean_products_blank_values` (135-143) | `clean_products` (89-98), `blank_to_null` (60-63) |

### 4.3 Clean - orders

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC22 | Day-first date `2/7/2015` = 2 July | P | `test_clean_orders_parses_day_first_dates` (146-149) | `clean_orders` (106-120), `to_date_dmy` (101-103) |
| TC23 | Leap day `29/2/2016` | P | `test_clean_orders_date_edge_cases` (152-159) | `to_date_dmy` (101-103) |
| TC24 | Date with extra spaces | P | `test_clean_orders_date_edge_cases` (152-159) | `to_date_dmy` (101-103) |
| TC25 | Date with leading zero `01/12/2016` | P | `test_clean_orders_date_edge_cases` (152-159) | `to_date_dmy` (101-103) |
| TC26 | Impossible date `31/2/2016` -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders` (106-120), `to_date_dmy` (101-103) |
| TC27 | Not a leap year `29/2/2017` -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC28 | Month 13 -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC29 | Wrong date format `2016-08-21` -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC30 | 2-digit year `21/8/16` -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC31 | Empty order date -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC32 | Missing order date -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `to_date_dmy` |
| TC33 | Missing order id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `blank_to_null` |
| TC34 | Blank order id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `blank_to_null` |
| TC35 | Missing customer id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `clean_id` |
| TC36 | Blank customer id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `clean_id` |
| TC37 | Missing product id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `clean_id` |
| TC38 | Blank product id -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders`, `clean_id` |
| TC39 | Text in profit (`n/a`) -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders` (106-120) |
| TC40 | Empty profit -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders` (106-120) |
| TC41 | Missing profit -> dropped | N | `test_clean_orders_drops_invalid_rows` (162-183) | `clean_orders` (106-120) |
| TC42 | Exact duplicate rows removed | N | `test_clean_orders_removes_duplicates` (186-188) | `clean_orders` (106-120) |
| TC43 | Same `row_id` with different values counted once | N | `test_clean_orders_same_row_id_counted_once` (191-194) | `clean_orders` (106-120) |
| TC44 | Bad ship date / quantity -> row kept with NULLs | N | `test_clean_orders_keeps_row_with_bad_optional_fields` (197-202) | `clean_orders` (106-120) |
| TC45 | All rows invalid -> 0 rows | N | `test_clean_orders_all_rows_invalid_gives_empty_result` (205-207) | `clean_orders` (106-120) |
| TC46 | Required column missing (schema change) -> job fails | N | `test_clean_orders_missing_required_column_fails` (210-214) | `clean_orders` (106-120) |

### 4.4 Reject reasons (used by data quality checks)

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC47 | Blank order id -> `missing order id` | N | `test_add_reject_reason` (218-227) | `add_reject_reason` (123-136) |
| TC48 | Missing customer id -> `missing customer id` | N | `test_add_reject_reason` (218-227) | `add_reject_reason` (123-136) |
| TC49 | Blank product id -> `missing product id` | N | `test_add_reject_reason` (218-227) | `add_reject_reason` (123-136) |
| TC50 | Impossible date -> `invalid order date` | N | `test_add_reject_reason` (218-227) | `add_reject_reason` (123-136) |
| TC51 | Text profit -> `invalid profit` | N | `test_add_reject_reason` (218-227) | `add_reject_reason` (123-136) |
| TC52 | Valid row has no reject reason | P | `test_add_reject_reason_valid_row_has_no_reason` (230-232) | `add_reject_reason` (123-136) |
| TC53 | Row with several problems -> first reason reported | N | `test_add_reject_reason_first_problem_is_reported` (235-238) | `add_reject_reason` (123-136) |
| TC54 | Rows without reason = rows kept by `clean_orders` | P | `test_reject_reason_matches_clean_orders` (241-246) | `add_reject_reason` (123-136), `clean_orders` (106-120) |

### 4.5 Enrich

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC55 | Profit rounded to 2 dp (`10.5678` -> `10.57`) | P | `test_enriched_orders_rounds_profit_to_2dp` (250-253) | `build_enriched_orders` (140-154) |
| TC56 | Negative profit rounded (`-14.9249` -> `-14.92`) | P | `test_enriched_orders_rounds_profit_to_2dp` (250-253) | `build_enriched_orders` (140-154) |
| TC57 | Small decimals rounded (`2.004` -> `2.0`) | P | `test_enriched_orders_rounds_profit_to_2dp` (250-253) | `build_enriched_orders` (140-154) |
| TC58 | Customer and product details added, order year added | P | `test_enriched_orders_adds_customer_and_product_columns` (256-261) | `build_enriched_orders` (140-154) |
| TC59 | Customer and product not found -> order kept as `Unknown` | N | `test_enriched_orders_keeps_orders_without_match` (264-267) | `build_enriched_orders` (140-154) |
| TC60 | Join keeps row count the same | P | `test_enriched_orders_row_count_unchanged` (270-272) | `build_enriched_orders` (140-154) |
| TC61 | IDs with different case/spaces still match | P | `test_enriched_orders_matches_ids_with_case_and_spaces` (275-279) | `build_enriched_orders` (140-154), `clean_id` (66-68) |
| TC62 | Customer found, product not found | N | `test_enriched_orders_partial_match` (282-286) | `build_enriched_orders` (140-154) |
| TC63 | Duplicate master rows do not multiply orders | N | `test_enriched_orders_duplicate_master_rows_do_not_duplicate_orders` (289-298) | `build_enriched_orders` (140-154), `clean_customers`, `clean_products` |
| TC64 | No orders -> 0 rows, no error | N | `test_enriched_orders_empty_orders` (301-303) | `build_enriched_orders` (140-154) |

### 4.6 Aggregate

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC65 | Profit by year, category, sub-category, customer | P | `test_profit_aggregate` (318-324) | `build_profit_aggregate` (158-163) |
| TC66 | Profit by year | P | `test_profit_by_year` (327-328) | `profit_by` (166-172) |
| TC67 | Profit by year and category | P | `test_profit_by_year_and_category` (331-333) | `profit_by` (166-172) |
| TC68 | Profit by customer | P | `test_profit_by_customer` (336-338) | `profit_by` (166-172) |
| TC69 | Profit by customer and year | P | `test_profit_by_customer_and_year` (341-343) | `profit_by` (166-172) |
| TC70 | Unknown products grouped as `Unknown` | N | `test_profit_aggregate_groups_unmatched_as_unknown` (346-353) | `build_profit_aggregate` (158-163) |
| TC71 | 0.10 + 0.20 = 0.3 (no floating point noise) | P | `test_profit_aggregate_no_floating_point_noise` (356-361) | `build_profit_aggregate` (158-163) |
| TC72 | No orders -> 0 rows, no error | N | `test_profit_aggregate_empty_input` (364-366) | `build_profit_aggregate` (158-163) |
| TC73 | Group by column that does not exist -> fails | N | `test_profit_by_unknown_column_fails` (369-372) | `profit_by` (166-172) |

### 4.7 End to end (real sample files)

| ID | Test case | Type | Test function (test file lines) | Actual code tested (code file lines) |
| --- | --- | --- | --- | --- |
| TC74 | Full pipeline on `Orders.json`, `Customer.xlsx`, `Products.csv`: no orders lost, no NULL keys, total profit matches at every step | P | `test_pipeline_on_sample_files` (378-395) | All functions: `read_*` (28-41) -> `clean_*` (78-120) -> `build_enriched_orders` (140-154) -> `build_profit_aggregate` (158-163) -> `profit_by` (166-172) |

### 4.8 Summary

| Area | Positive | Negative | Total |
| --- | --- | --- | --- |
| Read and save | 6 | 1 | 7 |
| Clean - customers and products | 5 | 9 | 14 |
| Clean - orders | 4 | 21 | 25 |
| Reject reasons | 2 | 6 | 8 |
| Enrich | 6 | 4 | 10 |
| Aggregate | 6 | 3 | 9 |
| End to end | 1 | 0 | 1 |
| **Total** | **30** | **44** | **74** |

(TC17 and TC20 check both good and bad rows; they are counted as negative above.)

---

## 5. Data quality rules on real tables (`notebooks/03_data_quality_checks`)

The unit tests above prove the logic with test data. The notebook below applies the same rules to the real tables on every load and saves the results to `dq_results`. Bad orders are saved to `rejected_orders` with a `reject_reason`.

| Rule | Level | Action | Notebook step | Same rule unit tested in |
| --- | --- | --- | --- | --- |
| Table is not empty | Critical | Job fails | Step 2 | TC19, TC45, TC64 |
| Required columns present (schema change) | Critical | Job fails | Step 2 | TC46 |
| Source file missing | Critical | Job fails | Step 1 | TC07 |
| Missing order / customer / product id | Reject | Row to `rejected_orders` | Step 3 | TC33-TC38, TC47-TC49 |
| Invalid order date | Reject | Row to `rejected_orders` | Step 3 | TC26-TC32, TC50 |
| Invalid profit | Reject | Row to `rejected_orders` | Step 3 | TC39-TC41, TC51 |
| Duplicate `row_id` | Reject | First row kept | Step 3 | TC42, TC43 |
| Rejected rows above limit (default 5%) | Critical | Job fails | Step 3 | - (threshold rule) |
| Missing customer / product id in master files | Reject | Row dropped | Step 3 | TC18, TC21 |
| Customer / product not found | Warn | Kept as `Unknown` | Step 5 | TC59, TC62, TC70 |
| Invalid ship date / quantity | Warn | Kept as NULL | Step 5 | TC44 |
| Customer name / country `Unknown` | Warn | Kept as `Unknown` | Step 5 | TC13-TC18 |
| Duplicate product id in source | Warn | One row kept | Step 5 | TC20, TC63 |
| Keys unique after cleaning | Critical | Job fails | Step 6 | TC17, TC20, TC63 |
| No NULL keys in `enriched_orders` | Critical | Job fails | Step 6 | TC74 |
| Raw = loaded + rejected + duplicates | Critical | Job fails | Step 6 | TC54, TC60 |
| Total profit reconciles across tables | Critical | Job fails | Step 6 | TC74 |

Status: the notebook has been created but **not run yet** on the real tables.
