# DataOps with Nessie, Iceberg, Spark and Great Expectations

Repo of the demo performed at [Subsurface 2023](https://www.dremio.com/subsurface/live/live2023/session/dataops-in-action-with-nessie-iceberg-and-great-expectations/).

## Getting started

1. build the docker image: `docker build . -t nessie-demo`
2. on one background shell run `./start-nessie.sh`
3. on another shell run `./start-shell`
4. Run the commands that are in the `commands` text file
5. Have fun 🐉

## Airflow implementation quickstart

1. Build the docker image: `./build-image.sh`
2. Run `docker compose up`
3. Access Airflow UI on `localhost:8080`, Spark UI on `localhost:4040`, Nessie UI on `localhost:19120`
4. Run the WAP_Demo DAG from the UI.