#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse # for command-line arguments
import os
import gzip

def main(params):
    user       = params.user
    password   = params.password
    host       = params.host
    port       = params.port
    db         = params.db
    table_name = params.table_name
    url        = params.url
    #csv_name  = params.local_file

    # Set csv_name based on whether the file is compressed or not
    if url.endswith(".gz"):
        # Remove the .gz extension from the file name
        csv_name = "output.csv"
        # Check if the gzip file exists before decompressing
        if not os.path.exists(f"{csv_name}.gz"):
            os.system(f"wget {url} -O {csv_name}.gz")
            # Decompress the gzip file
            with gzip.open(f"{csv_name}.gz", "rb") as f_in:
                with open(csv_name, "wb") as f_out:
                    f_out.write(f_in.read())
    else:
        # If not compressed, set csv_name to the original file name
        csv_name = "output.csv"

    # Download csv file from url (if not already downloaded)
    if not os.path.exists(csv_name):
        os.system(f"wget {url} -O {csv_name}")



    # connect to Postgres
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    # read csv file in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)
    # read first chunk
    df = next(df_iter)
    # convert datetime columns to datetime type
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    # create table
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    # insert first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # insert remaining chunks
    while True:
        try:
            start = time()
            batch = next(df_iter)
            batch['lpep_pickup_datetime'] = pd.to_datetime(batch['lpep_pickup_datetime'])
            batch['lpep_dropoff_datetime'] = pd.to_datetime(batch['lpep_dropoff_datetime'])
            batch.to_sql(name=table_name, con=engine, if_exists='append')
            end = time()
            print("Inserted another chunk.., took: ", end-start, " seconds")

        except StopIteration:
            break

# run the script:
if __name__ == '__main__':
    # user, password, host, port, db name, table name, url of csv file
    parser = argparse.ArgumentParser(description='Ingest data to Postgres.')

    parser.add_argument('--user', help= 'user name for Postgres')
    parser.add_argument('--password', help= 'password for Postgres')
    parser.add_argument('--host', help= 'host for Postgres')
    parser.add_argument('--port', help= 'port for Postgres')
    parser.add_argument('--db', help= 'db name for Postgres')
    parser.add_argument('--table_name', help= 'table name for Postgres')
    #parser.add_argument('--local_file', help= 'local path to the csv file')
    parser.add_argument('--url', help= 'url of csv file')

    args = parser.parse_args()

    main(args)




