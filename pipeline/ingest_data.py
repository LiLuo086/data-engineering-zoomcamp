#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Define data types for columns
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}
parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


@click.command()
@click.option("--pg_user", default="root", help="Postgres username")
@click.option("--pg_password", default="root", help="Postgres password")
@click.option("--pg_host", default="localhost", help="Postgres host")
@click.option("--pg_port", default="5432", help="Postgres port")
@click.option("--pg_db", default="ny_taxi", help="Postgres database")
@click.option("--year", default=2021, help="Year of data to ingest")
@click.option("--month", default=1, help="Month of data to ingest")
@click.option("--chunk_size", default=100000, help="Chunk size for reading data")
@click.option(
    "--target_table", default="yellow_taxi_data", help="Target table name in Postgres"
)
def run(
    pg_user, pg_password, pg_host, pg_port, pg_db, year, month, chunk_size, target_table
):
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"

    df_iter = pd.read_csv(
        url,
        dtype=dtype,  # type: ignore
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size,
    )  # type: ignore

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    )

    first = True

    for df_chunk in tqdm(df_iter):
        # Create table schema (no data)
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False
            print("Table created")

        # Insert chunks
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")
        print("Inserted:", len(df_chunk))


if __name__ == "__main__":
    run()
