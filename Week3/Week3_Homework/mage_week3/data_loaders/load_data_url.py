import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url_jan = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet"###
    url_feb = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet"
    url_mar = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet"
    url_apr = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet"
    url_may = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet"
    url_jun = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet"
    url_jul = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet"
    url_aug = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet"
    url_sep = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet"
    url_oct = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet"
    url_nov = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet"
    url_dec = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet"

    urls = [
        url_jan, url_feb, url_mar, url_apr, url_may, url_jun,
        url_jul, url_aug, url_sep, url_oct, url_nov, url_dec
    ]

    type_casting_dict = {
                'VendorID':             pd.Int64Dtype(),
                'passenger_count':      pd.Int64Dtype(),
                'trip_distance':        float,
                'RatecodeID':           pd.Int64Dtype(),
                'store_and_fwd_flag':   str,
                'PULocationID':         pd.Int64Dtype(),
                'DOLocationID':         pd.Int64Dtype(),
                'payment_type':         pd.Int64Dtype(),
                'fare_amount':          float,
                'extra':                float,
                'mta_tax':              float,
                'tip_amount':           float,
                'tolls_amount':         float,
                'improvement_surcharge':float,
                'total_amount':         float,
                'congestion_surcharge': float
    }

    date_columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    data_frames = [] # store files after read in for later conatenation

    # read urls in loop and concatenate frames
    for url in urls:
        response = requests.get(url)

        if response.status_code == 200:

            df = pd.read_parquet(url)
            # Apply type casting
            for col, dtype in type_casting_dict.items():
                if col in df.columns:
                    df[col] = df[col].astype(dtype)

            # Parse date columns
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

            data_frames.append(df)
        else:
            print(f"Failed to fetch data from {url}!")

    print(f"{69*'~'}\nHoooooray, your data has been piped successfully!\n{69*'~'}")

    data = pd.concat(
                data_frames, 
                ignore_index=True
    )
    print(data.info())

    return data



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
