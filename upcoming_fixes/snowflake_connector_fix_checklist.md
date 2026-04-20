# Snowflake Connector Fix Checklist

Use this checklist while updating `snowflake_connector.py`.

## 1) Dynamic SQL safety for stage/table/file values (High)

- [ ] Confirm whether `stage_name`, `table_name`, and `file_path` can ever come from user input or external data.
- [ ] Create a strict allowlist for known stage names and table names.
- [ ] Validate object names with a strict regex (letters, digits, underscore).
- [ ] Reject values that fail validation before building SQL.
- [ ] Use Snowflake-safe identifier handling patterns (avoid raw string concatenation for object names).

Why to do this:

- Prevents SQL injection/object-name injection risks.
- Avoids broken commands when values contain unexpected characters.

What to search (Google/Docs):

- `Snowflake Python connector SQL injection best practices`
- `Snowflake identifier quoting table name stage name`
- `Snowflake COPY INTO syntax stage path`
- `Snowflake PUT command syntax`

Useful docs:

- Snowflake Python Connector docs: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector
- COPY INTO table: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
- PUT command: https://docs.snowflake.com/en/sql-reference/sql/put
- Object identifiers: https://docs.snowflake.com/en/sql-reference/identifiers-syntax

## 2) Close both cursor and connection (High)

- [+] Store connection in a variable (not only cursor).
- [+] Close cursor in `finally`.
- [+] Close connection in `finally`.
- [+] Prefer context managers where possible so cleanup is automatic.

Why to do this:

- Prevents leaked Snowflake sessions/resources.
- Improves reliability in long-running scripts/jobs.

What to search (Google/Docs):

- `Snowflake Python connector close connection cursor`
- `Python context manager database connection`

Useful docs:

- Python Connector API: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
- Python context managers (`with`): https://docs.python.org/3/reference/compound_stmts.html#the-with-statement

## 3) Windows-safe filename extraction (Medium)

- [+] Replace manual split logic with OS-aware path handling.
- [ ] Use standard library path utilities for filename extraction.
- [ ] Test with both Windows (`\\`) and POSIX (`/`) style paths.

Why to do this:

- Ensures the correct staged filename is used in `COPY INTO`.
- Prevents path-format related load failures on Windows.

What to search (Google/Docs):

- `python os.path.basename windows path`
- `python pathlib Path name cross platform`

Useful docs:

- os.path docs: https://docs.python.org/3/library/os.path.html
- pathlib docs: https://docs.python.org/3/library/pathlib.html

## 4) Error handling should not silently hide failures (Medium)

- [ ] Decide a clear failure contract: raise exceptions or return structured status.
- [ ] Keep logging/printing for context, then re-raise or propagate error.
- [ ] Ensure caller can detect failure and react (retry/alert/abort).

Why to do this:

- Prevents false-success behavior.
- Makes failures visible to upstream code and monitoring.

What to search (Google/Docs):

- `python exception handling best practices reraise`
- `python logging exceptions logger.exception`

Useful docs:

- Python exceptions tutorial: https://docs.python.org/3/tutorial/errors.html
- Python logging docs: https://docs.python.org/3/library/logging.html

## 5) Validate required environment variables early (Medium)

- [ ] Define required env vars (`snowflake_user`, `snowflake_password`, `snowflake_account`, etc.).
- [ ] On startup, check for missing/empty values.
- [ ] Raise a clear error listing missing keys.
- [ ] Avoid logging secret values.

Why to do this:

- Fails fast with actionable errors.
- Prevents confusing runtime connection errors later.

What to search (Google/Docs):

- `python validate required environment variables`
- `python dotenv load_dotenv best practices`

Useful docs:

- python-dotenv: https://pypi.org/project/python-dotenv/
- os.getenv docs: https://docs.python.org/3/library/os.html

## 6) Remove or use unused config fields (Low)

- [ ] Check whether `snowflake_url` is truly needed.
- [ ] Remove it if unused, or connect it correctly if required by your setup.

Why to do this:

- Reduces confusion and maintenance debt.
- Keeps configuration surface minimal and clear.

What to search (Google/Docs):

- `python remove unused variables refactor`
- `snowflake python connector account parameter`

Useful docs:

- Snowflake connector connect params: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect

## Final validation pass

- [ ] Run a local test upload with a small CSV file.
- [ ] Verify PUT succeeds and COPY INTO loads expected row count.
- [ ] Test a failure path (bad stage/table) and confirm errors propagate clearly.
- [ ] Confirm no credentials are printed in logs/output.
