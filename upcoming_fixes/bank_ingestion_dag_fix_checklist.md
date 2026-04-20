# Bank Ingestion DAG Review Checklist (with line numbers)

File reviewed: dags/bank_ingestion.py

## 1) XCom payload can become too large (High)

Where to find it:

- Line 65: `extract_country_data` returns a full list of records.
- Line 101: `save_to_snowflake(all_extracted_data)` receives all mapped outputs.
- Line 176: dynamic mapping expands extraction per country.

Why to fix:

- Large datasets in XCom can exceed backend limits and fail DAG runs.

How to fix:

- [ ] Move persistence earlier: write each mapped task output directly to Snowflake (or object storage), not to XCom.
- [ ] Pass only small metadata in XCom (row counts, file path, batch id).
- [ ] Keep reduce/final task lightweight.

What to search:

- `Airflow XCom size limit best practices`
- `Airflow dynamic task mapping large payload`
- `Airflow avoid large XCom`

Docs:

- XComs: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
- Dynamic task mapping: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html

## 2) Sensors may occupy workers while waiting (High)

Where to find it:

- Line 38: API sensor decorator.
- Line 50: Snowflake sensor decorator.

Why to fix:

- Default poke behavior can tie up worker slots and reduce throughput.

How to fix:

- [ ] Use `mode="reschedule"` (or deferrable sensors where supported).
- [ ] Add retries with backoff for transient errors.
- [ ] Keep timeout and poke interval aligned with SLA.

What to search:

- `Airflow sensor mode reschedule vs poke`
- `Airflow deferrable sensors`

Docs:

- Sensors: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html

## 3) No idempotency guard in Snowflake load (High)

Where to find it:

- Line 143: `PUT` to stage with overwrite.
- Line 147: `COPY INTO world_bank_raw`.

Why to fix:

- Reruns/backfills may duplicate rows if target table does not dedupe.

How to fix:

- [ ] Load to staging table first.
- [ ] Use `MERGE` into final table with business keys (`country_code`, `indicator_code`, `year`).
- [ ] Add batch/run id for traceability and safe replay.

What to search:

- `Snowflake COPY INTO duplicate rows rerun`
- `Snowflake MERGE from staging table`
- `idempotent ETL Snowflake`

Docs:

- COPY INTO table: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
- MERGE: https://docs.snowflake.com/en/sql-reference/sql/merge

## 4) API sensor marks success without checking HTTP status (Medium)

Where to find it:

- Line 41 to 43: request is made.
- Line 44: success is returned immediately.

Why to fix:

- HTTP 4xx/5xx can still return a response object and be treated as healthy.

How to fix:

- [ ] Validate `response.status_code`.
- [ ] Call `response.raise_for_status()` before done=True.
- [ ] Optionally validate expected JSON shape.

What to search:

- `python requests raise_for_status best practice`
- `Airflow sensor API health check`

Docs:

- requests quickstart: https://requests.readthedocs.io/en/latest/user/quickstart/

## 5) JSON parse failures are not handled (Medium)

Where to find it:

- Line 76: `data = response.json()`.
- Line 93: only `RequestException` is caught.

Why to fix:

- Invalid JSON/body can raise decode errors and fail task unexpectedly.

How to fix:

- [ ] Catch JSON decode errors separately.
- [ ] Log country/indicator/status code for diagnosis.
- [ ] Skip/continue safely per indicator when bad payload appears.

What to search:

- `python requests json decode error handling`
- `robust API ingestion error handling python`

Docs:

- requests JSON handling: https://requests.readthedocs.io/en/latest/user/quickstart/#json-response-content

## 6) Possible pagination truncation (Medium)

Where to find it:

- Line 72: request uses `per_page=100`.
- Line 79 onward: processes only first returned page array.

Why to fix:

- If total pages > 1, you silently miss data.

How to fix:

- [ ] Read pagination metadata from response (`pages`, `page`, `total`).
- [ ] Loop through all pages until complete.
- [ ] Add logging when multi-page responses are detected.

What to search:

- `World Bank API pagination format=json`
- `World Bank indicators API pages per_page`

Docs:

- World Bank API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation

## 7) Snowflake provider import unresolved in environment (Medium)

Where to find it:

- Line 10: `from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook`

Why to fix:

- Missing provider package can break DAG parse/runtime in that environment.

How to fix:

- [ ] Install and pin `apache-airflow-providers-snowflake` compatible with your Airflow version.
- [ ] Use constraints files for reproducible installs.
- [ ] Add a DAG parse check in CI.

What to search:

- `apache-airflow-providers-snowflake compatibility matrix`
- `airflow constraints file pip install providers`

Docs:

- Snowflake provider docs: https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html
- Airflow installation constraints: https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html

## 8) Cursor lifecycle not explicit in connection check (Low)

Where to find it:

- Line 54: `conn.cursor().execute("SELECT 1")`.

Why to fix:

- Explicit cursor close is safer and cleaner for DB resource handling.

How to fix:

- [ ] Store cursor in a variable.
- [ ] Close cursor in `finally`.
- [ ] Then close connection.

What to search:

- `python db cursor close best practice`
- `snowflake connector cursor lifecycle`

Docs:

- Snowflake Python Connector API: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api

## Final validation checklist

- [ ] Run one full DAG execution and confirm all mapped country tasks succeed.
- [ ] Verify row counts in Snowflake match expected counts.
- [ ] Trigger a rerun and confirm no duplicate records (idempotency test).
- [ ] Test one simulated API failure and confirm clear retries/failure behavior.
- [ ] Confirm Airflow workers are not blocked by long sensor waits.
