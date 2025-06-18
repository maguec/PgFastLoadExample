# PgFastLoader

An example of writing lots of JSON data really fast into Postgres

- uses multiprocessing (threading) for many concurrent connections
- uses batch writes to postgres

## Prerequisites 

- [UV](https://github.com/astral-sh/uv)
- Python
- [Podman](https://podman.io/) if you want to play with Postgres locally


## Start up Postgres 

```bash
podman run  --rm --name pgfastloader -e "POSTGRES_PASSWORD=FastLoad3RR" -e "POSTGRES_DB=fastload" -p 5432:5432 docker.io/postgres:17
```

## Create the table

```bash
export PGPASSWORD=FastLoad3RR
psql -U postgres -h 127.0.0.1  fastload < db_setup.sql
```

## Generate the JSON file

```bash
uv run ./data_gen.py
```

## Load the data

run with the docker with the default config or change the parameters

```bash
uv run ./main.py -h
```
