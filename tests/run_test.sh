#!/usr/bin/env bash

set -e

#docker-compose up -d
#sleep 5

dub build -b debug

export TEST_USER=root
export TEST_PASSWORD=r00tme
export TEST_DATABASE=root
export TEST_DATABASE_HOST=127.0.0.1
export TEST_DATABASE_PORT=5432

dub -b debug

# cockroachdb
export IS_COCKROACH=true
export TEST_DATABASE=postgres
export TEST_DATABASE_HOST=127.0.0.1
export TEST_DATABASE_PORT=26257

dub -b debug