#!/bin/bash
docker run --entrypoint /bin/bash -v $(pwd):/demo --network nessie-demo -it nessie-demo:latest