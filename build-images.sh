#!/bin/bash

docker build -f SparkWorkerDockerfile -t spark-extended .
docker build -f AirflowExtendedDockerfile -t airflow-extended . 