#!/usr/bin/env bash
set -e

cd ..
dbt deps
dbt seed --full-refresh
dbt run
dbt test

