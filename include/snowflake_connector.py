import snowflake.connector
import os
from contextlib import contextmanager
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeConnector:
    # build a connector class to connect to snowflake and load data into it using snowflake hook from airflow
    def __init__(self):
        from dotenv import load_dotenv

        load_dotenv()
        self.snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    @contextmanager
    def conn(self):
        # Connect to Snowflake
        conn = None
        try:
            conn = self.snowflake_hook.get_conn()
            yield conn
        finally:
            if conn is not None:
                conn.close()
                print("Snowflake connection closed.")

    # def __init__(self):
    #     from dotenv import load_dotenv

    #     load_dotenv()
    #     self.snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    # @contextmanager
    # def conn(self):
    #     # Connect to Snowflake
    #     try:
    #         conn = snowflake.connector.connect(
    #             user=self.snowflake_user,
    #             password=self.snowflake_password,
    #             account=self.snowflake_account,
    #             warehouse=self.snowflake_warehouse,
    #             database=self.snowflake_database,
    #             schema=self.snowflake_schema,
    #         )
    #         yield conn
    #     finally:
    #         if conn is not None:
    #             conn.close()
    #             print("Snowflake connection closed.")

    # def _load_table_from_stage(self, cs, stage_name, table_name, file_name):
    #     cs.execute(f"COPY INTO {table_name} FROM @{stage_name}/{file_name}")

    # def put_file(self, file_path, stage_name, table_name):
    #     with self._conn() as conn:
    #         with conn.cursor() as cs:
    #             try:
    #                 cs.execute(f"PUT file://{file_path} @{stage_name}")
    #                 print(f"File {file_path} uploaded to stage {stage_name}")

    #                 self._load_table_from_stage(
    #                     cs, stage_name, table_name, os.path.basename(file_path)
    #                 )
    #                 print(
    #                     f"Data loaded from stage {stage_name} into table {table_name}"
    #                 )

    #             except Exception as e:
    #                 print(f"Failed to upload and load file: {e}")
