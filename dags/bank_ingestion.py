# bank ingestion DAG
import requests
from airflow.sdk import dag, task, Asset
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
from pathlib import Path
import time
import tempfile
import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# csv_bank_data = Dataset("data/world_bank_data.csv")

countries_codes = {
    "Egypt": "EGY",
    "Saudi Arabia": "SAU",
    "United Arab Emirates": "ARE",
    "Qatar": "QAT",
    "Kuwait": "KWT",
}

indicators = {
    "GDP": "NY.GDP.MKTP.CD",
    # 'Population': 'SP.POP.TOTL',
    "Unemployment": "SL.UEM.TOTL.ZS",
    "Inflation": "FP.CPI.TOTL.ZG",
    # 'Life Expectancy': 'SP.DYN.LE00.IN'
}


@dag(
    dag_id="bank_ingestion",
    schedule=timedelta(days=365),
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def bank_ingestion():
    @task.sensor(poke_interval=60, timeout=3600)
    def check_api_availability():
        try:
            response = requests.get(
                "https://api.worldbank.org/v2/countries?format=json", timeout=10
            )
            response.raise_for_status()
            return PokeReturnValue(is_done=True, xcom_value="API is online")
        except requests.exceptions.RequestException:
            return PokeReturnValue(is_done=False)

    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    @task.sensor(poke_interval=60, timeout=3600)
    def check_snowflake_connection():
        try:
            conn = snowflake_hook.get_conn()
            conn.cursor().execute("SELECT 1")
            print("Snowflake connection successful.")
            conn.close()
            return PokeReturnValue(is_done=True, xcom_value="Snowflake is online")

        except Exception as e:
            print(f"Snowflake connection failed: {e}")
            return PokeReturnValue(is_done=False)

    # that will .expand()
    @task
    def extract_country_data(country_tuple):
        country_name, country_code = country_tuple
        all_data = []

        # loop through indicators and fetch data for each indicator for the given country
        for ind_name, indicator in indicators.items():
            time.sleep(1)
            url = f"https://api.worldbank.org/v2/countries/{country_code}/indicators/{indicator}?format=json&per_page=100"

            try:
                response = requests.get(url, timeout=10)
                data = response.json()

                # check if data is valid and has the expected structure
                if len(data) > 1 and data[1]:
                    for data_row in data[1]:
                        if data_row.get("value") is not None:
                            row_data = {
                                "country_code": data_row["countryiso3code"],
                                "country_name": data_row["country"]["value"],
                                "indicator_code": data_row["indicator"]["id"],
                                "indicator_name": data_row["indicator"]["value"],
                                "year": data_row["date"],
                                "value": data_row["value"],
                                "date_retrieved": pd.Timestamp.now().isoformat(),
                            }

                            all_data.append(row_data)
            except requests.exceptions.RequestException as e:
                print(f"Failed to fetch {ind_name} for {country_name}: {e}")

        return all_data

    # check snowflake connection

    @task(outlets=[Asset("snowflake://world_bank_raw")])
    def save_to_snowflake(all_extracted_data):
        full_data = []
        for c_list in all_extracted_data:

            if c_list:
                full_data.extend(c_list)

        if not full_data:
            print("No extracted data to load into Snowflake.")
            return

        # df = pd.DataFrame(full_data)
        # create df columns in the order of the Snowflake table
        df = pd.DataFrame(
            full_data,
            columns=[
                "country_code",
                "country_name",
                "indicator_code",
                "indicator_name",
                "year",
                "value",
                "date_retrieved",
            ],
        )

        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        temp_csv_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".csv",
                newline="",
                encoding="utf-8",
                delete=False,
            ) as temp_file:
                temp_csv_path = temp_file.name
                df.to_csv(temp_file, index=False)

            cursor.execute(
                f"PUT file://{temp_csv_path} @my_int_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            )
            cursor.execute(
                f"""
                COPY INTO world_bank_raw
                FROM @my_int_stage/{Path(temp_csv_path).name}.gz
                """
            )

            print(f"Successfully loaded rows into Snowflake.")
        finally:
            cursor.close()
            conn.close()

            if temp_csv_path:
                Path(temp_csv_path).unlink(missing_ok=True)

    # @task(outlets=[csv_bank_data])
    # def save_csv(all_extracted_data):
    #     full_data = []
    #     for c_list in all_extracted_data:

    #         for ic_dict in c_list:
    #             full_data.append(ic_dict)

    #     df = pd.DataFrame(full_data)
    #     df.to_csv("data/world_bank_data.csv", index=False)

    api_check = check_api_availability()
    snowflake_check = check_snowflake_connection()

    country_codes_list = list(countries_codes.items())

    extracted_data = extract_country_data.expand(country_tuple=country_codes_list)

    [api_check, snowflake_check] >> extracted_data

    snowflake_save = save_to_snowflake(extracted_data)


bank_ingestion()
