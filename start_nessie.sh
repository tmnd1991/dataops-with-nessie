#!/bin/sh
docker network create nessie-demo
docker run --rm --name nessie --network nessie-demo -p 19120:19120 projectnessie/nessie:0.50.0
