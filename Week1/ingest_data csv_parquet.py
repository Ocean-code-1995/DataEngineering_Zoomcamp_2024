#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    output_file = "output"  # Default output file name

    # Check if the file is compressed (ends with .gz) or Parquet (ends with .parquet)
    if url.endswith(".gz"):
        output_file = "output.csv"  # Change the output file name
        os.system(f"wget {url} -O {output_file}")
        os.system(f"gunzip {output_file}")
    elif url.endswith(".parquet"):
        output_file = "output.parquet"  # Change the output file name
        os.system(f"wget {url} -O {output_file}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    if output_file.endswith(".csv"):
        df_iter = pd.read_csv(output_file, iterator=True, chunksize=100_000)
    elif output_file.endswith(".parquet"):
        df_iter = pd.read_parquet(output_file, engine='pyarrow', chunksize=100_000)
    else:
        raise ValueError("Unsupported file format")

    df = next(df_iter)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            start = time()
            batch = next(df_iter)
            batch['tpep_pickup_datetime'] = pd.to_datetime(batch['tpep_pickup_datetime'])
            batch['tpep_dropoff_datetime'] = pd.to_datetime(batch['tpep_dropoff_datetime'])
            batch.to_sql(name=table_name, con=engine, if_exists='append')
            end = time()
            print("Inserted another chunk.., took: ", end - start, " seconds")

        except StopIteration:
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres.')

    parser.add_argument('--user', help='user name for Postgres')
    parser.add_argument('--password', help='password for Postgres')
    parser.add_argument('--host', help='host for Postgres')
    parser.add_argument('--port', help='port for Postgres')
    parser.add_argument('--db', help='db name for Postgres')
    parser.add_argument('--table_name', help='table name for Postgres')
    parser.add_argument('--url', help='url of data file (CSV or Parquet)')

    args = parser.parse_args()

    main(args)
