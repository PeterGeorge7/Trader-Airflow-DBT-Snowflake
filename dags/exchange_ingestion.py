from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
import requests
import os
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

country_currency_codes_dict = {
    "EGP": "Egypt",
    "SAR": "Saudi Arabia",
    "AED": "United Arab Emirates",
    "QAR": "Qatar",
    "KWD": "Kuwait",
}


@dag(
    dag_id="exchange_ingestion",
    # make it schedule to run once a month # but what if month has 31 days? or 28/29 days? maybe we should use cron expression instead
    schedule="@monthly",  # schedule=timedelta(days=30) is not ideal because of varying month lengths, using cron expression for monthly schedule make it run on the first day of each month
    start_date=datetime(2026, 4, 1),
    catchup=False,
)
def exchange_ingestion():

    @task.sensor(poke_interval=60, timeout=3600)
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
    def extract_exchange_rates():
        open_exchange_api_key = os.getenv("open_exchange_api_key")
        country_currency_codes = ",".join(country_currency_codes_dict.keys())

        all_data = []
        for year in range(2010, 2027):
            for month in range(1, 13):
                if year == 2026 and month > 2:
                    break
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
                                "country": country_currency_codes_dict.get(
                                    currency_code
                                ),
                                "target_currency_code": currency_code,
                                "usd_exchange_rate": exchange_rate,
                                "date_timestamp": data["timestamp"],
                                "date": f"{year}-{month:02d}-01",
                                "source": "open_exchange_rates",
                            }
                        )
                    print(
                        f"Successfully fetched exchange rates for {year}-{month:02d}."
                    )
                    all_data.extend(exchange_rates)
                except requests.exceptions.RequestException as e:
                    raise Exception(f"Failed to fetch exchange rates: {e}")

        # save into csv file
        df = pd.DataFrame(all_data)
        # df.to_csv("/data/exchange_rate.csv")
        # CSV output path is hardcoded to /data, which can fail outside container/Linux setups (especially on Windows):
        # make output path safer for local + container use.
        output_path = os.path.join(os.getcwd(), "exchange_rate.csv")
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def load_to_snowflake(output_path=None):
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = snowflake_hook.get_conn()
        if not output_path:
            output_path = os.path.join(os.getcwd(), "exchange_rate.csv")
        print(f"Loading exchange rates from {output_path} into Snowflake...")
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                PUT file://{output_path} @my_int_stage AUTO_COMPRESS=TRUE
                """
            )
            cursor.execute(
                f"""
                    COPY INTO exchange_rate
                    FROM @my_int_stage/{os.path.basename(output_path)}.gz
                """
            )
            print(f"Successfully loaded exchange rates into Snowflake.")
        except Exception as e:
            raise Exception(f"Failed to load exchange rates into Snowflake: {e}")
        finally:
            cursor.close()
            conn.close()

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
