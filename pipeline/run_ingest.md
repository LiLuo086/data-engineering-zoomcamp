# Ingest data

## Ingest taxi data to postgre database

docker run -it \
  --network= \
  taxi_ingest:v001 \
  --pg_user="root" \
  --pg_password="root" \
  --pg_host="pgdatabase" \
  --pg_port=5432 \
  --pg_db='ny_taxi' \
  --year=2021 \
  --month=1 \
  --chunk_size=100000 \
  --target_table="yellow_taxi_data"

## Ingest zone lookup data to postgre database

docker run -it --rm \
  --network=pipeline_default \
  zone_ingest:v002 \
  --pg_user="root" \
  --pg_password="root" \
  --pg_host="pgdatabase" \
  --pg_port=5432 \
  --pg_db='ny_taxi' \
  --chunk_size=100000 \
  --target_table="zones"
