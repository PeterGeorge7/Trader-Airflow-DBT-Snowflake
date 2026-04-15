import requests
import pandas as pd
import time
def world_bank_api(country_code,indicator):
    url = f'https://api.worldbank.org/v2/countries/{country_code}/indicators/{indicator}?format=json&per_page=100'
    
    time.sleep(1)  # Wait 1 second between requests
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        if len(data) > 1 and data[1]:
            print(f"Success! {len(data[1])}")
            return data[1]
        else:
            print(f"no data")
            return []
    except requests.exceptions.RequestException as e:
        print(f"API Extraction failed: {e}")
        return None
    

countries_codes = {
    'Egypt': 'EGY',
    'Saudi Arabia': 'SAU',
    'United Arab Emirates': 'ARE',
    'Qatar': 'QAT',
    'Kuwait': 'KWT'
}

indicators = {
    'GDP': 'NY.GDP.MKTP.CD',
    # 'Population': 'SP.POP.TOTL',
    'Unemployment': 'SL.UEM.TOTL.ZS',
    'Inflation': 'FP.CPI.TOTL.ZG',
    # 'Life Expectancy': 'SP.DYN.LE00.IN'
}

def save_to_csv():
    all_data = []
    for country in countries_codes.values():
        for indicator in indicators.values():

            data = world_bank_api(country, indicator)
            if data:
                for data_row in data:
                    # save country code, indicator code, year, and value to a CSV file
                    row_data = {
                        'country_code': data_row['countryiso3code'],
                        'country_name': data_row['country']['value'],
                        'indicator_code': data_row['indicator']['id'],
                        'indicator_name': data_row['indicator']['value'],
                        'year': data_row['date'],
                        'value': data_row['value'],
                        'date_retrieved': pd.Timestamp.now()
                    }

                    if row_data['value'] is not None:
                        all_data.append(row_data)
                        
    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv('world_bank_data.csv', index=False)
        print("Data saved to world_bank_data.csv")
    else:
        print("No data to save.")

if __name__ == "__main__":
    save_to_csv()