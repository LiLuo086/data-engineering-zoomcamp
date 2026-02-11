#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Define data types for columns
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}


@click.command()
@click.option("--pg_user", default="root", help="Postgres username")
@click.option("--pg_password", default="root", help="Postgres password")
@click.option("--pg_host", default="localhost", help="Postgres host")
@click.option("--pg_port", default="5432", help="Postgres port")
@click.option("--pg_db", default="ny_taxi", help="Postgres database")
@click.option("--chunk_size", default=100000, help="Chunk size for reading data")
@click.option("--target_table", default="zones", help="Target table name in Postgres")
def run(pg_user, pg_password, pg_host, pg_port, pg_db, chunk_size, target_table):
    """Ingest Zone lookup data into PostgreSQL database."""

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    df_iter = pd.read_csv(
        url,
        dtype=dtype,  # type: ignore
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
