#!/bin/bash

gcloud config set project $1
bq show sepsis3 > /dev/null || bq --location=US mk -d sepsis3
echo "running extract-48h-of-hourly-case-lab-series.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./extract-48h-of-hourly-case-lab-series.sql)"
echo "running extract-48h-of-hourly-case-vital-series.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./extract-48h-of-hourly-case-vital-series.sql)"
echo "running extract-48h-of-hourly-control-lab-series.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./extract-48h-of-hourly-control-lab-series.sql)"
echo "running extract-48h-of-hourly-control-vital-series.sql..."
bq query -n 0 --use_legacy_sql=false "$(< ./extract-48h-of-hourly-control-vital-series.sql)"