import snowflake.connector
import os
from contextlib import contextmanager


class SnowflakeConnector:
    def __init__(self):
        from dotenv import load_dotenv

        load_dotenv()
        self.snowflake_url = os.getenv("snowflake_url")
        self.snowflake_user = os.getenv("snowflake_user")
        self.snowflake_password = os.getenv("snowflake_password")
        self.snowflake_account = os.getenv("snowflake_account")
        self.snowflake_warehouse = os.getenv("snowflake_warehouse")
        self.snowflake_database = os.getenv("snowflake_database")
        self.snowflake_schema = os.getenv("snowflake_schema")

    @contextmanager
    def _conn(self):
        # Connect to Snowflake
        try:
            conn = snowflake.connector.connect(
                user=self.snowflake_user,
                password=self.snowflake_password,
                account=self.snowflake_account,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema,
            )
            yield conn
        finally:
            if conn is not None:
                conn.close()
                print("Snowflake connection closed.")

    def _load_table_from_stage(self, cs, stage_name, table_name, file_name):
        cs.execute(f"COPY INTO {table_name} FROM @{stage_name}/{file_name}")

    def put_file(self, file_path, stage_name, table_name):
        with self._conn() as conn:
            with conn.cursor() as cs:
                try:
                    cs.execute(f"PUT file://{file_path} @{stage_name}")
                    print(f"File {file_path} uploaded to stage {stage_name}")

                    self._load_table_from_stage(
                        cs, stage_name, table_name, os.path.basename(file_path)
                    )
                    print(
                        f"Data loaded from stage {stage_name} into table {table_name}"
                    )

                except Exception as e:
                    print(f"Failed to upload and load file: {e}")
