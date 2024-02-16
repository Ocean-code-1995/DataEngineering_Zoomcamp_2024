import dlt
import pandas as pd
import numpy as np


@dlt.resource(write_disposition="append")
def fetch_data_resource_monthly(years: list, months: list) -> None:
    """Generator function to retrieve the yellow taxi data month by month.
    args:
        - years: list of years to fetch data for
        - months: list of months to fetch data for
    returns:
        - None
    yields:
        - pd.DataFrame: monthly yellow taxi data
    """
    for year in years:
        print(f"Fetching data for year {year}")
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"

            try:
                monthly_data = pd.read_parquet(path=url, engine="pyarrow")

                # Ensure the airport_fee column is treated as float64, converting None to NaN
                # ---> since the column is not present in all files,
                # ---> and there a data type mismatches for that column across the monthly files (if present)
                if 'airport_fee' in monthly_data.columns:
                    monthly_data['airport_fee'] = monthly_data['airport_fee'].astype(float)
                else:
                    # If the column is missing, add it as float64 with NaN values
                    monthly_data['airport_fee'] = np.nan

                print(f" - Successfully retrieved file from {year} - {month}")
                yield monthly_data

            except Exception as e:
                print(f" x Failed to retrieve file from {year}-{month}: {e}")
        print(f" -> Finished fetching data for year {year}\n")




# run the pipeline
if __name__ == "__main__":
    print(">>>>>>>>> INITIALIZING PIPELINE <<<<<<<<<")
    # Create a pipeline
    pipeline = dlt.pipeline(
        pipeline_name = 'yellow_taxi_pipeline',
        destination   = 'filesystem',
        dataset_name  = 'yellow_taxi_data'
    )
    # Initialize the generator & pass parameters
    generator = fetch_data_resource_monthly(
        years  = [2019, 2020],
        months = [f"{i:02d}" for i in range(1, 13)]
    )
    # Run the pipeline
    load_info = pipeline.run(data=generator)
    print(load_info)
