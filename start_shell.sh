#!/bin/bash
docker run --entrypoint /bin/bash -v $(pwd):/demo --network dataops-with-nessie_gdpr-demo -it nessie-demo:latest