import requests
import numpy as np
import pandas as pd
import time


def fetch_data_api(trip_obj):
    """
    Fetch data from the source
    :param trip_obj: dict containing the type of trip as string and the years to fetch as a list
    :return: response containing the fetched data
    """
    trip_type = trip_obj['type']
    for year in trip_obj['years']:
        print(f"Fetching data for year {year}")
        for month in range(1, 13):
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{trip_type}_tripdata_{year}-{month:02d}.parquet'
            print(f"Downloading {trip_type} trip data for {year}-{month:02d}")
            response = requests.get(url)
            response.raise_for_status()
            monthly_df = pd.read_parquet(url, engine='pyarrow')
            # in case of yellow taxi and fhv trip data, extra columns need to handled
            if trip_type == 'yellow':
                if 'airport_fee' in monthly_df.columns:
                    monthly_df['airport_fee'] = monthly_df['airport_fee'].astype(float)
                else:
                    monthly_df['airport_fee'] = np.nan
            if trip_type == 'fhv':
                if 'p_ulocation_id' in monthly_df.columns:
                    monthly_df['PUlocationID'] = monthly_df['PUlocationID'].astype(float)
                else:
                    monthly_df['PUlocationID'] = np.nan
                if 'd_olocation_id' in monthly_df.columns:
                    monthly_df['DOlocationID'] = monthly_df['DOlocationID'].astype(float)
                else:
                    monthly_df['DOlocationID'] = np.nan
            yield monthly_df
        print(f" -> Downloaded {trip_type} trip data for {year}\n")
    print(f" ---> Downloaded {trip_type} trip data for all years")

