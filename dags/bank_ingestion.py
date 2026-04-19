# bank ingestion DAG
import requests
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.sensors.base import PokeReturnValue
import time
import pandas as pd

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
    dag_id="bankapi_ingestion",
    schedule=timedelta(years=1),
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
            return PokeReturnValue(is_done=True, xcom_value="API is online")
        except requests.exceptions.RequestException:
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

    @task()
    def ingest_data(all_extracted_data):
        full_data = []
        for c_list in all_extracted_data:

            for ic_dict in c_list:
                full_data.append(ic_dict)

        df = pd.DataFrame(full_data)
        df.to_csv("data/world_bank_data.csv", index=False)

    api_check = check_api_availability()

    country_codes_list = list(countries_codes.items())

    extracted_data = extract_country_data.expand(country_tuple=country_codes_list)

    api_check >> extracted_data

    saved_file_path = ingest_data(extracted_data)


bank_ingestion()
