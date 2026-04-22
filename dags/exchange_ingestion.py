from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
import requests
import os
import pandas as pd
from include.snowflake_connector import SnowflakeConnector
from include.constants import abs_output_data_path, country_currency_codes_dict

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="exchange_ingestion",
    # make it schedule to run once a month # but what if month has 31 days? or 28/29 days? maybe we should use cron expression instead
    schedule="@monthly",  # schedule=timedelta(days=30) is not ideal because of varying month lengths, using cron expression for monthly schedule make it run on the first day of each month
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
)
def exchange_ingestion():

    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")  #
    def check_api_availability():
        open_exchange_api_key = os.getenv("open_exchange_api_key")
        try:
            response = requests.get(
                f"https://openexchangerates.org/api/latest.json?app_id={open_exchange_api_key}",
                timeout=10,
            )
            response.raise_for_status()
            return PokeReturnValue(is_done=True, xcom_value="API is online")
        except requests.exceptions.RequestException:
            return PokeReturnValue(is_done=False)

    # this should gets the exchange rate of each currency against USD and store it in a list of dictionaries with keys: currency_code, exchange_rate, date
    @task
    def extract_exchange_rates(ds=None):
        open_exchange_api_key = os.getenv("open_exchange_api_key")
        country_currency_codes = ",".join(country_currency_codes_dict.keys())

        # make the year of the airflow schedule dynamic, so it only fetches data up to the current month of the current year, and not beyond that, since future data is not available.
        # how to make it depends on the airflow schedule? we want to make sure it only fetches data up to the current month of the current year, and not beyond that, since future data is not available.
        execution_date = pd.to_datetime(ds)
        year = execution_date.year
        month = execution_date.month
        print(f"Extracting exchange rates for year: {year}, month: {month}")
        try:
            response = requests.get(
                f"https://openexchangerates.org/api/historical/{year}-{month:02d}-01.json?app_id={open_exchange_api_key}&symbols={country_currency_codes}",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            exchange_rates = []
            for currency_code, exchange_rate in data["rates"].items():
                exchange_rates.append(
                    {
                        "country_name": country_currency_codes_dict.get(currency_code),
                        "country_currency_code": currency_code,
                        "usd_exchange_rate": exchange_rate,
                        "date_timestamp": data["timestamp"],
                        "date": f"{year}-{month:02d}-01",
                        "source": "open_exchange_rates",
                    }
                )
            print(f"Successfully fetched exchange rates for {year}-{month:02d}.")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch exchange rates: {e}")

        # save into csv file
        df = pd.DataFrame(exchange_rates)
        # df.to_csv("/data/exchange_rate.csv")
        # CSV output path is hardcoded to /data, which can fail outside container/Linux setups (especially on Windows):
        # make output path safer for local + container use.
        output_dir = os.getenv("OUTPUT_DATA_PATH", abs_output_data_path)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "exchange_rate.csv")
        df.to_csv(output_path, index=False)
        return output_path
        # print extraction year and month for debugging
        # print(f"Extracting exchange rates for execution date: {ds}")
        # date from string to datetime
        # execution_date = pd.to_datetime(ds)
        # year = execution_date.year
        # month = execution_date.month
        # print(f"Extracting exchange rates for year: {year}, month: {month}")

    @task
    def load_to_snowflake(output_path=None):
        snowflake_connector = SnowflakeConnector()
        target_table_exchange = "exchange_rate"
        temp_table_exchange = f"{target_table_exchange}_temp"
        if not output_path:
            output_path = os.path.join(
                os.getenv("OUTPUT_DATA_PATH", abs_output_data_path),
                "exchange_rate.csv",
            )
        with snowflake_connector.conn() as conn:
            cursor = conn.cursor()
            try:
                # Create a temporary table with the same structure as the target table
                cursor.execute(
                    f"""
                    CREATE OR REPLACE TABLE {temp_table_exchange} LIKE {target_table_exchange}
                    """
                )
                print(f"Temporary table {temp_table_exchange} created successfully.")

                # Upload the CSV file to Snowflake stage
                cursor.execute(
                    f"""
                    PUT file://{output_path} @my_int_stage AUTO_COMPRESS=TRUE
                    """
                )
                print(f"File {output_path} uploaded to stage successfully.")

                # Load data from the stage into the temporary table
                cursor.execute(
                    f"""
                    COPY INTO {temp_table_exchange}
                    FROM @my_int_stage/{os.path.basename(output_path)}.gz
                    """
                )
                print(
                    f"Data loaded into temporary table {temp_table_exchange} successfully."
                )
                print(
                    f"Starting merge of data from {temp_table_exchange} into {target_table_exchange}..."
                )
                # Merge data from the temporary table into the target table
                cursor.execute(
                    f"""
                    MERGE INTO {target_table_exchange} AS target
                    USING {temp_table_exchange} AS source
                    ON target.country_currency_code = source.country_currency_code
                        AND target.date = source.date
                    WHEN MATCHED THEN UPDATE SET
                        target.usd_exchange_rate = source.usd_exchange_rate,
                        target.date_timestamp = source.date_timestamp,
                        target.source = source.source
                    WHEN NOT MATCHED THEN INSERT (
                        country_name, country_currency_code, usd_exchange_rate, date_timestamp, date, source
                    ) VALUES (
                        source.country_name, source.country_currency_code, source.usd_exchange_rate, source.date_timestamp, source.date, source.source
                    )
                    """
                )
                print(
                    f"Data merged into target table {target_table_exchange} successfully."
                )
            except Exception as e:
                raise Exception(f"Failed to load exchange rates into Snowflake: {e}")
            finally:
                # Clean up: Drop the temporary table and remove the file from stage
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table_exchange}")
                cursor.execute(
                    f"REMOVE @my_int_stage/{os.path.basename(output_path)}.gz"
                )
                # Remove the local CSV file after loading
                if os.path.exists(output_path):
                    os.remove(output_path)
                    print(f"Local file {output_path} removed successfully.")

    @task.bash()
    def dbt_models_exchange():
        return "dbt run --profiles-dir /usr/local/airflow/dbt/trader_dbt --project-dir /usr/local/airflow/dbt/trader_dbt --select stg_exchange_rate+"

    api_check = check_api_availability()
    extracted_data = extract_exchange_rates()
    (
        api_check
        >> extracted_data
        >> load_to_snowflake(extracted_data)
        >> dbt_models_exchange()
    )


exchange_ingestion()
