#!/bin/bash

gcloud config set project $1
bq show sepsis3 > /dev/null || bq --location=US mk -d sepsis3
echo "runing static-query.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./static-query.sql)"
echo "runing Cohort.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./Cohort.sql)"
echo "running hourly_cohort.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./hourly_cohort.sql)"