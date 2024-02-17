import dlt
import numpy as np
import pandas as pd
from dlt.sources.helpers import requests
import time


@dlt.resource(write_disposition='replace')
def fetch_data_resource(trip_obj):
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


if __name__ == "__main__":
    # define the needed data
    needed_data = [
        {'type': 'green', 'years': [2019, 2020]},
        {'type': 'yellow', 'years': [2019, 2020]},
        {'type': 'fhv', 'years': [2019]}
    ]
    # configure the pipeline with your destination details
    start = time.time()
    for collection in needed_data:
        pipeline = dlt.pipeline(
            pipeline_name=f'tlc_{collection["type"]}_data',
            destination='filesystem',
            dataset_name=f'tlc_{collection["type"]}_data'
        )
        print(f'Running pipeline for {collection["type"]} trip data')
        run = pipeline.run(data=fetch_data_resource(collection))
        print(run)
        print(f'Pipeline for {collection["type"]} trip data completed')
        print('--')
    end = time.time()
    print(f'All done! in{str(end - start)} seconds')